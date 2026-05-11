import asyncio
import argparse
import sys
import uuid
from pathlib import Path
from collections import defaultdict

# Add Backend root to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from sqlalchemy import select

from app.database import _get_session_factory, init_db
from app.llm.pipelines import apply_codebook_to_interview
from app.models import (
    Codebook,
    CodebookThemeRelationship,
    Corpus,
    CorpusChunk,
    CorpusDocument,
    DocumentAnalysis,
    Theme,
    ThemeOccurrence,
)
from app.schemas.llm import InterviewAnalysisResult

INTERVIEWS = {
    "UserA": """
Interviewer: How is the new software?
Participant: It crashes every hour. I lose my work constantly. It's incredibly unstable. On top of that, I know management spent a fortune on this, and it feels like a total waste of money.
    """,
    "UserB": """
Interviewer: How is the new software?
Participant: When it works, it is very fast. The data processing speed is much better than the old system. However, it freezes at least twice a day, which is frustrating.
    """,
    "UserC": """
Interviewer: How is the new software?
Participant: The rollout was a disaster. No training, just a sudden switch. It's very buggy and crashes often. But I do like the new shared dashboards; they make working with the remote team much easier.
    """
}

async def seed_db():
    print("Seeding database with sample interviews and codebook...")
    
    # Initialize DB (creates tables if missing)
    await init_db()
    
    factory = _get_session_factory()
    async with factory() as session:
        # Create Corpus
        project_id = uuid.uuid4()
        corpus = Corpus(project_id=project_id, name="Deployment Feedback Interviews (Batch)")
        session.add(corpus)
        await session.flush()

        # Create Codebook
        codebook = Codebook(project_id=str(project_id), name="Software Deployment Evaluation", version=1, created_by="admin")
        session.add(codebook)
        await session.flush()

        # Create Themes
        themes_data = [
            ("Poor Change Management", "Mentions of poor communication, lack of training, or feeling rushed during the rollout."),
            ("Steep Learning Curve", "Comments indicating the system is difficult to learn, confusing, or non-intuitive initially."),
            ("Performance & Efficiency", "Positive remarks about the speed, data processing capabilities, or time saved."),
            ("Collaboration Benefits", "Positive remarks about working with others, shared dashboards, or reduced siloing."),
            ("System Instability", "Mentions of crashes, bugs, freezing, or data loss."),
            ("Cost Concerns", "Comments questioning the financial value, price, or ROI of the system.")
        ]

        for label, desc in themes_data:
            theme = Theme(label=label, description=desc)
            session.add(theme)
            await session.flush()
            
            rel = CodebookThemeRelationship(codebook_id=codebook.id, theme_id=theme.id)
            session.add(rel)

        # Create Documents and Chunks for multiple users
        for user, text in INTERVIEWS.items():
            doc = CorpusDocument(corpus_id=corpus.id, title=f"Participant {user}")
            session.add(doc)
            await session.flush()
            
            chunk = CorpusChunk(document_id=doc.id, text=text.strip(), chunk_index=0)
            session.add(chunk)

        await session.commit()
        
        print("\n--- SEED SUCCESSFUL ---")
        print(f"Corpus ID: {corpus.id}")
        print(f"Codebook ID: {codebook.id}")
        print("Use these IDs with --corpus-id and --codebook-id for the batch analysis step.")

async def _analyze_single_document(session, doc_id: uuid.UUID, codebook_id: uuid.UUID):
    # 1. Check if already analyzed
    existing_analysis_result = await session.execute(
        select(DocumentAnalysis).where(
            DocumentAnalysis.document_id == doc_id,
            DocumentAnalysis.codebook_id == codebook_id
        )
    )
    existing_analysis = existing_analysis_result.scalar_one_or_none()

    if existing_analysis:
        print(f"\n[HINT] Document '{doc_id}' has already been analyzed with this codebook!")
        user_input = input("Do you want to rerun the analysis and overwrite existing results? [y/N]: ")
        if user_input.lower() != 'y':
            print("Skipping document.")
            return False
        # Delete old analysis
        await session.delete(existing_analysis)
        await session.flush()
        print("Old analysis deleted. Proceeding...\n")

    # 2. Load Document Text
    chunks_result = await session.execute(
        select(CorpusChunk).where(CorpusChunk.document_id == doc_id).order_by(CorpusChunk.chunk_index)
    )
    chunks = chunks_result.scalars().all()
    
    if not chunks:
        print(f"Error: Document '{doc_id}' has no text chunks.")
        return False
        
    transcript = "\n".join([c.text for c in chunks])

    # 3. Load Codebook Themes
    theme_rels_result = await session.execute(
        select(CodebookThemeRelationship).where(CodebookThemeRelationship.codebook_id == codebook_id)
    )
    theme_rels = theme_rels_result.scalars().all()
    
    theme_ids = [rel.theme_id for rel in theme_rels]
    if not theme_ids:
        print("Error: Codebook has no themes.")
        return False

    themes_result = await session.execute(
        select(Theme).where(Theme.id.in_(theme_ids))
    )
    themes = themes_result.scalars().all()

    if not themes:
        print("Error: Codebook has no themes.")
        return False

    # Format codebook context
    codebook_lines = []
    theme_map = {} # label -> theme_id for saving later
    for t in themes:
        codebook_lines.append(f"Theme: {t.label}\nDefinition: {t.description}\n")
        theme_map[t.label.lower()] = t.id
    
    codebook_context = "\n".join(codebook_lines)

    print(f"Applying codebook '{codebook_id}' ({len(themes)} themes) to document '{doc_id}' ({len(transcript)} chars)...", flush=True)

    try:
        result: InterviewAnalysisResult = apply_codebook_to_interview(transcript, codebook_context)
    except Exception as e:
        print(f"Error during LLM invocation: {e}", flush=True)
        return False

    # 4. Save to Database
    analysis = DocumentAnalysis(
        document_id=doc_id,
        codebook_id=codebook_id,
        summary=result.summary,
        researcher_notes=result.researcher_notes
    )
    session.add(analysis)
    await session.flush()

    for t_res in result.themes:
        # Find matching theme ID by label (case-insensitive approximation)
        matched_theme_id = theme_map.get(t_res.theme_label.lower())
        if not matched_theme_id:
            # Fallback if LLM slightly altered the label
            for db_label, db_id in theme_map.items():
                if t_res.theme_label.lower() in db_label or db_label in t_res.theme_label.lower():
                    matched_theme_id = db_id
                    break
        
        if matched_theme_id:
            occ = ThemeOccurrence(
                analysis_id=analysis.id,
                theme_id=matched_theme_id,
                is_present=t_res.present,
                confidence=t_res.confidence,
                quote=t_res.quote if t_res.present else None
            )
            session.add(occ)

    await session.commit()
    
    print(f"\n--- ANALYSIS COMPLETE & SAVED TO DB FOR DOC {doc_id} ---")
    if result.summary:
        print(f"Summary: {result.summary}")
    return True

async def analyze_document(doc_id_str: str, codebook_id_str: str):
    doc_id = uuid.UUID(doc_id_str)
    codebook_id = uuid.UUID(codebook_id_str)

    await init_db()
    factory = _get_session_factory()
    async with factory() as session:
        await _analyze_single_document(session, doc_id, codebook_id)

async def analyze_corpus(corpus_id_str: str, codebook_id_str: str):
    corpus_id = uuid.UUID(corpus_id_str)
    codebook_id = uuid.UUID(codebook_id_str)

    await init_db()
    factory = _get_session_factory()
    
    async with factory() as session:
        # Get all documents in the corpus
        docs_result = await session.execute(
            select(CorpusDocument).where(CorpusDocument.corpus_id == corpus_id)
        )
        documents = docs_result.scalars().all()

        if not documents:
            print("Error: Corpus has no documents.")
            return

        print(f"Found {len(documents)} documents in corpus '{corpus_id}'. Starting batch analysis...")
        
        # Analyze each document
        for doc in documents:
            print(f"\n[{doc.title}]")
            await _analyze_single_document(session, doc.id, codebook_id)
            
        # Aggregate results
        print("\n" + "="*50)
        print("BATCH EXPERIMENT RESULTS: THEME FREQUENCIES")
        print("="*50)
        
        stmt = (
            select(ThemeOccurrence, Theme, CorpusDocument)
            .join(Theme, ThemeOccurrence.theme_id == Theme.id)
            .join(DocumentAnalysis, ThemeOccurrence.analysis_id == DocumentAnalysis.id)
            .join(CorpusDocument, DocumentAnalysis.document_id == CorpusDocument.id)
            .where(DocumentAnalysis.codebook_id == codebook_id)
            .where(CorpusDocument.corpus_id == corpus_id)
            .where(ThemeOccurrence.is_present == True)
        )
        
        results = await session.execute(stmt)
        rows = results.all()
        
        theme_to_docs = defaultdict(list)
        all_theme_labels = set()
        
        # Fetch all themes for codebook to show 0 counts
        theme_rels_result = await session.execute(
            select(Theme.label)
            .join(CodebookThemeRelationship, Theme.id == CodebookThemeRelationship.theme_id)
            .where(CodebookThemeRelationship.codebook_id == codebook_id)
        )
        for label in theme_rels_result.scalars().all():
            all_theme_labels.add(label)

        for occ, theme, doc in rows:
            theme_to_docs[theme.label].append(doc.title)
            
        for label in sorted(all_theme_labels):
            docs = theme_to_docs.get(label, [])
            count = len(docs)
            print(f"Theme: {label:<25} | Present in {count}/{len(documents)} interviews")
            if count > 0:
                print(f"       -> Found in: {', '.join(docs)}")
        print("\nFinished!")

def main():
    parser = argparse.ArgumentParser(description="DB-integrated Codebook Analysis")
    parser.add_argument("--seed", action="store_true", help="Seed the database with multiple sample interviews and a codebook.")
    parser.add_argument("--document-id", type=str, help="UUID of a single CorpusDocument to analyze.")
    parser.add_argument("--corpus-id", type=str, help="UUID of a Corpus to analyze in batch.")
    parser.add_argument("--codebook-id", type=str, help="UUID of the Codebook to apply.")
    
    args = parser.parse_args()

    if args.seed:
        asyncio.run(seed_db())
    elif args.corpus_id and args.codebook_id:
        asyncio.run(analyze_corpus(args.corpus_id, args.codebook_id))
    elif args.document_id and args.codebook_id:
        asyncio.run(analyze_document(args.document_id, args.codebook_id))
    else:
        print("Please provide --seed, OR --corpus-id + --codebook-id, OR --document-id + --codebook-id.")
        parser.print_help()

if __name__ == "__main__":
    main()
