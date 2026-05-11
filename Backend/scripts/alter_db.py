import asyncio
import sys
from pathlib import Path

# Add Backend root to path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from sqlalchemy import text
from app.database import _get_engine

async def alter_db():
    engine = _get_engine()
    async with engine.begin() as conn:
        try:
            await conn.execute(text("ALTER TABLE themes ADD COLUMN description TEXT;"))
            print("Successfully added 'description' column to 'themes' table.")
        except Exception as e:
            print(f"Error (might already exist): {e}")

if __name__ == "__main__":
    asyncio.run(alter_db())
