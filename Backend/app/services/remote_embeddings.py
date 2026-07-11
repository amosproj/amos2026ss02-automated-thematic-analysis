from __future__ import annotations

import math

import httpx

from app.config import Settings, get_settings
from app.llm import providers


class RemoteEmbeddingClient:
    """OpenAI-compatible embeddings client for the selected embedding provider."""

    def __init__(
        self,
        settings: Settings | None = None,
        client: httpx.AsyncClient | None = None,
        provider: str | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client
        self._owns_client = client is None
        self._provider = provider

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> RemoteEmbeddingClient:
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.aclose()

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        provider_id = providers.normalize(self._provider) or providers.resolve_default(self._settings)
        spec = providers.get_provider(provider_id)
        if spec is None:
            raise RuntimeError(f"Embedding provider '{provider_id}' is not configured.")

        if not spec.supports_embeddings:
            spec = providers.get_provider(providers.DEFAULT_PROVIDER_ID)
            if spec is None:
                raise RuntimeError("Default embedding provider is not configured.")

        api_key = getattr(self._settings, spec.api_key_attr)
        if not api_key:
            raise RuntimeError(f"{spec.api_key_attr} is required for {spec.label} embeddings.")
        model_name = getattr(self._settings, spec.embedding_model_attr, None)
        if not model_name:
            raise RuntimeError(f"{spec.embedding_model_attr} is required for {spec.label} embeddings.")

        base_url = getattr(self._settings, spec.base_url_attr).rstrip("/")
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        client = self._client
        if client is None:
            client = httpx.AsyncClient(timeout=self._settings.LLM_REQUEST_TIMEOUT_S)
            self._client = client

        embeddings: list[list[float]] = []
        for batch in self._chunked(texts, max(1, self._settings.EMBEDDING_BATCH_SIZE)):
            payload = {
                "model": model_name,
                "input": batch,
            }
            response = await client.post(f"{base_url}/embeddings", json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

            rows = sorted(data.get("data", []), key=lambda item: item.get("index", 0))
            embeddings.extend(list(map(float, row["embedding"])) for row in rows)
        return embeddings

    @staticmethod
    def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
        return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)
