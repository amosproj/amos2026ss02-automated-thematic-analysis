import asyncio
import sys
from pathlib import Path

# Add Backend root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from app.database import _get_engine


async def alter_db():
    engine = _get_engine()
    
    # We use IF NOT EXISTS so this is safe to run multiple times,
    # and we commit after each statement so a failure in one
    # doesn't poison the transaction for the rest.
    statements = [
        "ALTER TABLE themes ADD COLUMN IF NOT EXISTS description TEXT;",
        "ALTER TABLE codebooks ADD COLUMN IF NOT EXISTS llm_tokens_input INTEGER;",
        "ALTER TABLE codebooks ADD COLUMN IF NOT EXISTS llm_tokens_output INTEGER;",
        "ALTER TABLE codebook_application_runs ADD COLUMN IF NOT EXISTS llm_tokens_input INTEGER;",
        "ALTER TABLE codebook_application_runs ADD COLUMN IF NOT EXISTS llm_tokens_output INTEGER;",
    ]

    async with engine.connect() as conn:
        for stmt in statements:
            try:
                await conn.execute(text(stmt))
                await conn.commit()
                print(f"Successfully executed: {stmt}")
            except Exception as e:
                # Some dialects (like older SQLite) don't support IF NOT EXISTS in ALTER TABLE
                # but postgres does. In case it fails, we rollback the sub-transaction and continue.
                await conn.rollback()
                print(f"Skipping statement (might already exist or be unsupported): {stmt}")
                print(f"  -> Error: {e}")

if __name__ == "__main__":
    asyncio.run(alter_db())
