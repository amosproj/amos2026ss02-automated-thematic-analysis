from __future__ import annotations

import math

import httpx

from app.config import Settings, get_settings


class RemoteEmbeddingClient:
    """OpenAI-compatible embeddings client for the FAU gateway."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if not self._settings.LLM_API_KEY_FAU:
            raise RuntimeError("LLM_API_KEY_FAU is required for FAU embeddings.")

        base_url = self._settings.LLM_BASE_URL_FAU.rstrip("/")
        payload = {
            "model": self._settings.EMBEDDING_MODEL_FAU,
            "input": texts,
        }
        headers = {
            "Authorization": f"Bearer {self._settings.LLM_API_KEY_FAU}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=self._settings.LLM_REQUEST_TIMEOUT_S) as client:
            response = await client.post(f"{base_url}/embeddings", json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        rows = sorted(data.get("data", []), key=lambda item: item.get("index", 0))
        return [list(map(float, row["embedding"])) for row in rows]


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)
