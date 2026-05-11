import asyncio
import argparse
import sys
import uuid
from pathlib import Path

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

LONG_INTERVIEW_TEXT = """
Interviewer: Thank you for taking the time to speak with me today about the new software deployment. Can you walk me through your initial reactions when the system was first rolled out?

Participant: Honestly, it was a bit of a nightmare at first. The communication from management was basically non-existent. We just showed up on Monday and the old system was gone. It felt very rushed.

Interviewer: That sounds frustrating. How did the lack of communication affect your day-to-day work?

Participant: Well, nobody knew how to log in for the first two hours! Once we finally got in, the interface was completely different. I spent most of that first week just trying to figure out where my daily reports went. The learning curve was extremely steep. But I will say, once you get the hang of it, it does process data a lot faster.

Interviewer: So there were some performance benefits once you learned it?

Participant: Yes, absolutely. The data processing is lightning fast compared to the legacy system. And I really love the new collaboration dashboard. Before, I had to email spreadsheets back and forth with the accounting team. Now, we can both look at the same dashboard in real-time, which has probably saved us ten hours a week in meetings.

Interviewer: That's a significant improvement. What about stability? Have you experienced any technical issues?

Participant: Unfortunately, yes. It crashes at least twice a week, usually right when I'm trying to export a large PDF report. It's incredibly annoying. You lose your unsaved progress and have to restart the whole application. Support says they are working on a patch, but it's been a month. Also, to be honest, I'm not sure the company got its money's worth. I heard this system cost twice as much as the old one, and with all these bugs, it doesn't feel worth it.
"""

async def seed_db():
    print("Seeding database with sample interview and codebook...")
    
    # Initialize DB (creates tables if missing)
    await init_db()
    
    factory = _get_session_factory()
    async with factory() as session:
        # Create Corpus
        project_id = uuid.uuid4()
        corpus = Corpus(project_id=project_id, name="Deployment Feedback Interviews")
        session.add(corpus)
        await session.flush()

        # Create Document
        doc = CorpusDocument(corpus_id=corpus.id, title="Participant 01 - Rollout Experience")
        session.add(doc)
        await session.flush()

        # Create Chunk (Storing the entire interview in one chunk for simplicity in this demo)
        chunk = CorpusChunk(document_id=doc.id, text=LONG_INTERVIEW_TEXT.strip(), chunk_index=0)
        session.add(chunk)

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

        await session.commit()
        
        print("\n--- SEED SUCCESSFUL ---")
        print(f"Document ID: {doc.id}")
        print(f"Codebook ID: {codebook.id}")
        print("Use these IDs for the analysis step.")

async def analyze_document(doc_id_str: str, codebook_id_str: str):
    doc_id = uuid.UUID(doc_id_str)
    codebook_id = uuid.UUID(codebook_id_str)

    await init_db()
    
    factory = _get_session_factory()
    async with factory() as session:
        # 1. Check if already analyzed
        existing_analysis_result = await session.execute(
            select(DocumentAnalysis).where(
                DocumentAnalysis.document_id == doc_id,
                DocumentAnalysis.codebook_id == codebook_id
            )
        )
        existing_analysis = existing_analysis_result.scalar_one_or_none()

        if existing_analysis:
            print("\n[HINT] This interview has already been analyzed with this codebook!")
            user_input = input("Do you want to rerun the analysis and overwrite existing results? [y/N]: ")
            if user_input.lower() != 'y':
                print("Aborting analysis.")
                return
            # Delete old analysis
            await session.delete(existing_analysis)
            await session.commit()
            print("Old analysis deleted. Proceeding...\n")

        # 2. Load Document Text
        chunks_result = await session.execute(
            select(CorpusChunk).where(CorpusChunk.document_id == doc_id).order_by(CorpusChunk.chunk_index)
        )
        chunks = chunks_result.scalars().all()
        
        if not chunks:
            print("Error: Document has no text chunks.")
            return
            
        transcript = "\n".join([c.text for c in chunks])

        # 3. Load Codebook Themes
        theme_rels_result = await session.execute(
            select(CodebookThemeRelationship).where(CodebookThemeRelationship.codebook_id == codebook_id)
        )
        theme_rels = theme_rels_result.scalars().all()
        
        theme_ids = [rel.theme_id for rel in theme_rels]
        if not theme_ids:
            print("Error: Codebook has no themes.")
            return

        themes_result = await session.execute(
            select(Theme).where(Theme.id.in_(theme_ids))
        )
        themes = themes_result.scalars().all()

        if not themes:
            print("Error: Codebook has no themes.")
            return

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
            sys.exit(1)

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
        
        print("\n--- ANALYSIS COMPLETE & SAVED TO DB ---")
        print(f"Analysis ID: {analysis.id}")
        if result.summary:
            print(f"\nSummary: {result.summary}")
        
        print("\nTheme Results Saved:")
        for t in result.themes:
            print(f" - {t.theme_label}: Present={t.present} (Conf: {t.confidence})")

def main():
    parser = argparse.ArgumentParser(description="DB-integrated Codebook Analysis")
    parser.add_argument("--seed", action="store_true", help="Seed the database with a dummy interview and codebook.")
    parser.add_argument("--document-id", type=str, help="UUID of the CorpusDocument to analyze.")
    parser.add_argument("--codebook-id", type=str, help="UUID of the Codebook to apply.")
    
    args = parser.parse_args()

    if args.seed:
        asyncio.run(seed_db())
    elif args.document_id and args.codebook_id:
        asyncio.run(analyze_document(args.document_id, args.codebook_id))
    else:
        print("Please provide either --seed OR both --document-id and --codebook-id.")
        parser.print_help()

if __name__ == "__main__":
    main()
