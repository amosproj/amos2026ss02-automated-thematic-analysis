import asyncio
import sys
from pathlib import Path

# Add Backend root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from app.database import _get_engine


async def alter_db():
    engine = _get_engine()
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE codebooks ADD COLUMN llm_tokens_input INTEGER;"))
            await conn.execute(text("ALTER TABLE codebooks ADD COLUMN llm_tokens_output INTEGER;"))
            print("Successfully added token columns to 'codebooks' table.")
        except Exception as e:
            print(f"Error for codebooks (might already exist): {e}")

        try:
            await conn.execute(text("ALTER TABLE codebook_application_runs ADD COLUMN llm_tokens_input INTEGER;"))
            await conn.execute(text("ALTER TABLE codebook_application_runs ADD COLUMN llm_tokens_output INTEGER;"))
            print("Successfully added token columns to 'codebook_application_runs' table.")
        except Exception as e:
            print(f"Error for codebook_application_runs (might already exist): {e}")

if __name__ == "__main__":
    asyncio.run(alter_db())
