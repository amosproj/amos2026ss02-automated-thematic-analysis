from __future__ import annotations

import json

import httpx

from app.config import Settings
from app.services.remote_embeddings import RemoteEmbeddingClient


async def test_remote_embeddings_batches_requests_and_preserves_order() -> None:
    seen_batches: list[list[str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        batch = list(payload["input"])
        seen_batches.append(batch)
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": index, "embedding": [float(text.rsplit("-", 1)[1])]}
                    for index, text in enumerate(batch)
                ]
            },
        )

    settings = Settings(
        DATABASE_URL="sqlite+aiosqlite:///:memory:",
        LLM_API_KEY_FAU="fau-key",
        LLM_BASE_URL_FAU="https://example.test/v1",
        EMBEDDING_BATCH_SIZE=2,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = RemoteEmbeddingClient(settings=settings, client=http_client)

        embeddings = await client.embed([f"text-{index}" for index in range(5)])

    assert seen_batches == [["text-0", "text-1"], ["text-2", "text-3"], ["text-4"]]
    assert embeddings == [[0.0], [1.0], [2.0], [3.0], [4.0]]
