"""Tests for automatic and on-demand transcript-to-demographic linking."""

INGESTION_API = "/api/v1/ingestion"
DEMOGRAPHIC_API = "/api/v1/demographic"
P1_STR = "00000000-0000-0000-0000-000000000001"


async def _create_corpus(client, name: str = "Corpus") -> str:
    resp = await client.post(
        f"{INGESTION_API}/corpora",
        json={"project_id": P1_STR, "name": name},
    )
    assert resp.status_code == 201
    return resp.json()["data"]["id"]


async def _bulk_ingest(client, corpus_id: str, titles: list[str]) -> None:
    docs = [{"title": t, "text": f"Interview transcript for {t}."} for t in titles]
    resp = await client.post(
        f"{INGESTION_API}/corpora/{corpus_id}/documents/bulk",
        json={"documents": docs},
    )
    assert resp.status_code == 201, resp.text


async def _upload_and_confirm_csv(client, corpus_id: str, usernames: list[str]) -> None:
    header = "username;age"
    rows = "\n".join(f"{u};30" for u in usernames)
    csv_content = f"{header}\n{rows}\n"
    upload = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        files={"file": ("demo.csv", csv_content, "application/octet-stream")},
    )
    assert upload.status_code == 201
    import_id = upload.json()["data"]["import_id"]
    confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert confirm.status_code == 201


async def _get_link_summary(client, corpus_id: str) -> dict:
    resp = await client.get(f"{DEMOGRAPHIC_API}/{corpus_id}/link-summary")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    return body["data"]


# ---------------------------------------------------------------------------
# Scenario 1: perfect match — case and whitespace variations
# ---------------------------------------------------------------------------


async def test_link_summary_perfect_match(client):
    corpus_id = await _create_corpus(client, "Perfect Match")

    # Doc titles have case variations; Pydantic strips whitespace on ingest
    await _bulk_ingest(client, corpus_id, [
        "Participant_01",   # uppercase first letter
        "PARTICIPANT_02",   # all caps
        "participant_03",   # clean lowercase
    ])

    # Demographic usernames have complementary case variations
    await _upload_and_confirm_csv(client, corpus_id, [
        "participant_01",   # lowercase — normalises to match "Participant_01"
        "participant_02",   # lowercase — normalises to match "PARTICIPANT_02"
        "PARTICIPANT_03",   # uppercase — normalises to match "participant_03"
    ])

    summary = await _get_link_summary(client, corpus_id)

    assert summary["total_transcripts"] == 3
    assert summary["matched"] == 3
    assert all(d["matched"] for d in summary["details"])


# ---------------------------------------------------------------------------
# Scenario 2: partial match — one document has no demographic row
# ---------------------------------------------------------------------------


async def test_link_summary_partial_match(client):
    corpus_id = await _create_corpus(client, "Partial Match")

    await _bulk_ingest(client, corpus_id, [
        "participant_01",
        "PARTICIPANT_02",   # case variation
        "participant_03",   # no matching demographic row
    ])

    # Only 2 demographic rows uploaded
    await _upload_and_confirm_csv(client, corpus_id, [
        "PARTICIPANT_01",   # normalises to match "participant_01"
        "participant_02",   # normalises to match "PARTICIPANT_02"
    ])

    summary = await _get_link_summary(client, corpus_id)

    assert summary["total_transcripts"] == 3
    assert summary["matched"] == 2


# ---------------------------------------------------------------------------
# Scenario 3: zero match — no username overlaps at all
# ---------------------------------------------------------------------------


async def test_link_summary_zero_match(client):
    corpus_id = await _create_corpus(client, "Zero Match")

    await _bulk_ingest(client, corpus_id, [
        "participant_01",
        "participant_02",
        "participant_03",
    ])

    # Completely different usernames
    await _upload_and_confirm_csv(client, corpus_id, [
        "other_01",
        "other_02",
        "other_03",
    ])

    summary = await _get_link_summary(client, corpus_id)

    assert summary["total_transcripts"] == 3
    assert summary["matched"] == 0
    assert all(not d["matched"] for d in summary["details"])
