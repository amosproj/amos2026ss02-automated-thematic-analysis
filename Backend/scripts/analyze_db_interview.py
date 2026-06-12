import argparse
import asyncio
import sys
import uuid
from collections import defaultdict
from pathlib import Path

# Add Backend root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import _get_session_factory, init_db
from app.models import (
    Code,
    CodeAssignment,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Corpus,
    CorpusDocument,
    DocumentCoding,
    Theme,
    ThemeAssignment,
    ThemeCodeRelationship,
)
from app.services.codebook_application import CodebookApplicationService

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

CODEBOOK_ITEMS = [
    (
        "Poor Change Management",
        "Mentions of poor communication, lack of training, or feeling rushed during the rollout.",
        "Rollout Communication Gap",
        "Comments about missing communication, preparation, or rollout guidance.",
    ),
    (
        "Steep Learning Curve",
        "Comments indicating the system is difficult to learn, confusing, or non-intuitive initially.",
        "Difficult Onboarding",
        "Mentions that the software was hard to learn or use at first.",
    ),
    (
        "Performance & Efficiency",
        "Positive remarks about the speed, data processing capabilities, or time saved.",
        "Improved Speed",
        "Positive comments about faster processing or saved time.",
    ),
    (
        "Collaboration Benefits",
        "Positive remarks about working with others, shared dashboards, or reduced siloing.",
        "Shared Visibility",
        "Comments about dashboards, collaboration, or easier cross-team work.",
    ),
    (
        "System Instability",
        "Mentions of crashes, bugs, freezing, or data loss.",
        "Crashes And Freezes",
        "Mentions of crashes, bugs, freezes, or lost work.",
    ),
    (
        "Cost Concerns",
        "Comments questioning the financial value, price, or ROI of the system.",
        "Questionable Value",
        "Concerns about cost, waste, ROI, or financial value.",
    ),
]


async def seed_db() -> None:
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
        codebook = Codebook(
            corpus_id=corpus.id,
            name="Software Deployment Evaluation",
            description="Sample codebook for deployment feedback interviews.",
            version=1,
            created_by="admin",
        )
        session.add(codebook)
        await session.flush()

        # Create Themes and Codes
        for theme_label, theme_desc, code_label, code_desc in CODEBOOK_ITEMS:
            theme = Theme(
                codebook_id=codebook.id,
                label=theme_label,
                description=theme_desc,
                is_active=True,
            )
            code = Code(
                codebook_id=codebook.id,
                label=code_label,
                description=code_desc,
                is_active=True,
            )
            session.add(theme)
            session.add(code)
            await session.flush()

            session.add_all([
                CodebookThemeRelationship(codebook_id=codebook.id, theme_id=theme.id),
                CodebookCodeRelationship(codebook_id=codebook.id, code_id=code.id),
                ThemeCodeRelationship(
                    codebook_id=codebook.id,
                    theme_id=theme.id,
                    code_id=code.id,
                ),
            ])

        # Create Documents for multiple users
        for user, text in INTERVIEWS.items():
            doc = CorpusDocument(corpus_id=corpus.id, title=f"Participant {user}", content=text.strip())
            session.add(doc)

        await session.commit()

        print("\n--- SEED SUCCESSFUL ---")
        print(f"Corpus ID: {corpus.id}")
        print(f"Codebook ID: {codebook.id}")
        print("Use these IDs with --corpus-id and --codebook-id for the batch analysis step.")


async def _load_document(session: AsyncSession, doc_id: uuid.UUID) -> CorpusDocument | None:
    result = await session.execute(select(CorpusDocument).where(CorpusDocument.id == doc_id))
    return result.scalar_one_or_none()


async def _has_existing_document_coding(
    session: AsyncSession,
    *,
    document_id: uuid.UUID,
    codebook_id: uuid.UUID,
) -> bool:
    existing_result = await session.execute(
        select(DocumentCoding.id).where(
            DocumentCoding.document_id == document_id,
            DocumentCoding.codebook_id == codebook_id,
        )
    )
    return existing_result.first() is not None


async def _analyze_single_document(
    session: AsyncSession,
    doc_id: uuid.UUID,
    codebook_id: uuid.UUID,
) -> bool:
    doc = await _load_document(session, doc_id)
    if not doc or not doc.content:
        print(f"Error: Document '{doc_id}' has no content.")
        return False

    if await _has_existing_document_coding(
        session,
        document_id=doc_id,
        codebook_id=codebook_id,
    ):
        print(f"\n[HINT] Document '{doc_id}' already has coding results for this codebook.")
        user_input = input("Do you want to run the current codebook again and create a new run? [y/N]: ")
        if user_input.lower() != "y":
            print("Skipping document.")
            return False

        print("Proceeding with a new codebook application run...\n")

    print(
        f"Applying codebook '{codebook_id}' to document '{doc_id}' ({len(doc.content)} chars)...",
        flush=True,
    )
    service = CodebookApplicationService(session)
    summary = await service.apply_codebook(
        corpus_id=doc.corpus_id,
        codebook_id=codebook_id,
        transcript_document_ids=[doc_id],
    )
    print(f"\n--- ANALYSIS COMPLETE & SAVED TO RUN {summary.application_run.id} ---")
    print(f"Documents coded: {summary.documents_coded}/{summary.documents_total}")
    if summary.failed_documents:
        print(f"Failed documents: {summary.failed_documents}")
    return True


async def analyze_document(doc_id_str: str, codebook_id_str: str) -> None:
    doc_id = uuid.UUID(doc_id_str)
    codebook_id = uuid.UUID(codebook_id_str)

    await init_db()
    factory = _get_session_factory()
    async with factory() as session:
        await _analyze_single_document(session, doc_id, codebook_id)


async def analyze_corpus(corpus_id_str: str, codebook_id_str: str) -> None:
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

        service = CodebookApplicationService(session)
        summary = await service.apply_codebook(
            corpus_id=corpus_id,
            codebook_id=codebook_id,
            transcript_document_ids=[doc.id for doc in documents],
        )

        # Aggregate results
        print("\n" + "="*50)
        print("BATCH EXPERIMENT RESULTS: THEME FREQUENCIES")
        print("="*50)

        stmt = (
            select(ThemeAssignment, Theme, CorpusDocument)
            .join(Theme, ThemeAssignment.theme_id == Theme.id)
            .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
            .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
            .where(DocumentCoding.application_run_id == summary.application_run.id)
            .where(CorpusDocument.corpus_id == corpus_id)
            .where(ThemeAssignment.is_present)
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

        for _occ, theme, doc in rows:
            theme_to_docs[theme.label].append(doc.title)

        for label in sorted(all_theme_labels):
            docs = theme_to_docs.get(label, [])
            count = len(docs)
            print(f"Theme: {label:<25} | Present in {count}/{len(documents)} interviews")
            if count > 0:
                print(f"       -> Found in: {', '.join(docs)}")

        code_stmt = (
            select(CodeAssignment, Code, CorpusDocument)
            .join(Code, CodeAssignment.code_id == Code.id)
            .join(DocumentCoding, CodeAssignment.document_coding_id == DocumentCoding.id)
            .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
            .where(DocumentCoding.application_run_id == summary.application_run.id)
            .where(CorpusDocument.corpus_id == corpus_id)
        )
        code_results = await session.execute(code_stmt)
        code_to_docs = defaultdict(list)
        for _assignment, code, doc in code_results.all():
            code_to_docs[code.label].append(doc.title)

        if code_to_docs:
            print("\nCODE ASSIGNMENTS")
            for label in sorted(code_to_docs):
                docs = code_to_docs[label]
                print(f"Code: {label:<25} | Assigned in {len(docs)}/{len(documents)} interviews")
                print(f"      -> Found in: {', '.join(docs)}")

        print(
            f"\nRun {summary.application_run.id}: "
            f"{summary.documents_coded} coded, {summary.documents_failed} failed"
        )
        print("\nFinished!")


def main() -> None:
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
