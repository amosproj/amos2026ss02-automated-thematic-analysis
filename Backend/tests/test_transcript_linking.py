"""Tests for automatic and on-demand transcript-to-demographic linking."""

INGESTION_API = "/api/v1/ingestion"
DEMOGRAPHIC_API = "/api/v1/demographic"
P1_STR = "00000000-0000-0000-0000-000000000001"


async def _create_corpus(client, name: str = "Corpus") -> str:
    resp = await client.post(
        f"{INGESTION_API}/corpora",
        json={"corpus_id": P1_STR, "name": name},
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


async def _link(client, corpus_id: str, document_id: str, row_id: str | None):
    return await client.put(
        f"{DEMOGRAPHIC_API}/{corpus_id}/documents/{document_id}/link",
        json={"demographic_row_id": row_id},
    )


async def _unlink(client, corpus_id: str, document_id: str):
    return await client.delete(
        f"{DEMOGRAPHIC_API}/{corpus_id}/documents/{document_id}/link"
    )


def _doc_id(summary: dict, title: str) -> str:
    return next(d["document_id"] for d in summary["details"] if d["document_title"] == title)


def _row_id(summary: dict, interviewee_id: str) -> str:
    return next(
        r["row_id"] for r in summary["demographic_rows"] if r["interviewee_id"] == interviewee_id
    )


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


# ---------------------------------------------------------------------------
# Enriched summary: demographic_rows are reported with link state
# ---------------------------------------------------------------------------


async def test_link_summary_includes_demographic_rows(client):
    corpus_id = await _create_corpus(client, "Rows Reported")
    await _bulk_ingest(client, corpus_id, ["alpha"])
    await _upload_and_confirm_csv(client, corpus_id, ["alpha", "beta"])

    summary = await _get_link_summary(client, corpus_id)

    rows = {r["interviewee_id"]: r for r in summary["demographic_rows"]}
    assert set(rows) == {"alpha", "beta"}
    # 'alpha' auto-linked to the transcript; 'beta' has no transcript.
    assert rows["alpha"]["linked"] is True
    assert rows["alpha"]["linked_document_id"] is not None
    assert rows["beta"]["linked"] is False
    assert rows["beta"]["linked_document_id"] is None


# ---------------------------------------------------------------------------
# Manual link / unlink
# ---------------------------------------------------------------------------


async def test_manual_link_unmatched_transcript(client):
    corpus_id = await _create_corpus(client, "Manual Link")
    await _bulk_ingest(client, corpus_id, ["typo_name"])
    await _upload_and_confirm_csv(client, corpus_id, ["real_name"])

    summary = await _get_link_summary(client, corpus_id)
    assert summary["matched"] == 0
    doc_id = _doc_id(summary, "typo_name")
    row_id = _row_id(summary, "real_name")

    resp = await _link(client, corpus_id, doc_id, row_id)
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert data["matched"] == 1
    detail = next(d for d in data["details"] if d["document_id"] == doc_id)
    assert detail["matched"] is True
    assert detail["demographic_row_id"] == row_id


async def test_manual_unlink(client):
    corpus_id = await _create_corpus(client, "Manual Unlink")
    await _bulk_ingest(client, corpus_id, ["participant_01"])
    await _upload_and_confirm_csv(client, corpus_id, ["participant_01"])

    summary = await _get_link_summary(client, corpus_id)
    assert summary["matched"] == 1
    doc_id = _doc_id(summary, "participant_01")

    resp = await _unlink(client, corpus_id, doc_id)
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert data["matched"] == 0
    detail = next(d for d in data["details"] if d["document_id"] == doc_id)
    assert detail["matched"] is False
    assert detail["demographic_row_id"] is None


async def test_manual_link_reassigns_row(client):
    """Linking a row already linked to another transcript moves the link (1:1)."""
    corpus_id = await _create_corpus(client, "Reassign")
    await _bulk_ingest(client, corpus_id, ["doc_a", "doc_b"])
    await _upload_and_confirm_csv(client, corpus_id, ["doc_a"])

    summary = await _get_link_summary(client, corpus_id)
    row_id = _row_id(summary, "doc_a")
    doc_a = _doc_id(summary, "doc_a")
    doc_b = _doc_id(summary, "doc_b")
    # doc_a auto-linked to the row.
    assert next(d for d in summary["details"] if d["document_id"] == doc_a)["matched"]

    # Move the link to doc_b.
    resp = await _link(client, corpus_id, doc_b, row_id)
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert data["matched"] == 1
    assert next(d for d in data["details"] if d["document_id"] == doc_b)["demographic_row_id"] == row_id
    assert next(d for d in data["details"] if d["document_id"] == doc_a)["demographic_row_id"] is None


async def test_manual_link_rejects_row_from_other_corpus(client):
    corpus_id = await _create_corpus(client, "Validation")
    await _bulk_ingest(client, corpus_id, ["doc_a"])
    await _upload_and_confirm_csv(client, corpus_id, ["doc_a"])
    summary = await _get_link_summary(client, corpus_id)
    doc_id = _doc_id(summary, "doc_a")

    bogus_row = "00000000-0000-0000-0000-0000000000ff"
    resp = await _link(client, corpus_id, doc_id, bogus_row)
    assert resp.status_code == 422
    assert resp.json()["success"] is False


async def test_manual_link_unknown_document_is_404(client):
    corpus_id = await _create_corpus(client, "Missing Doc")
    await _upload_and_confirm_csv(client, corpus_id, ["doc_a"])
    summary = await _get_link_summary(client, corpus_id)
    row_id = _row_id(summary, "doc_a")

    bogus_doc = "00000000-0000-0000-0000-0000000000aa"
    resp = await _link(client, corpus_id, bogus_doc, row_id)
    assert resp.status_code == 404
    assert resp.json()["success"] is False


# ---------------------------------------------------------------------------
# Manual overrides survive a subsequent link-summary read
#
# get_link_summary must NOT re-run auto-linking; otherwise a manual unlink of a
# title-matched transcript (or a reassign away from one) is silently reverted on
# the next board/list load.
# ---------------------------------------------------------------------------


async def test_manual_unlink_survives_link_summary_reload(client):
    corpus_id = await _create_corpus(client, "Unlink Persists")
    await _bulk_ingest(client, corpus_id, ["participant_01"])
    await _upload_and_confirm_csv(client, corpus_id, ["participant_01"])

    summary = await _get_link_summary(client, corpus_id)
    assert summary["matched"] == 1  # auto-linked on title match
    doc_id = _doc_id(summary, "participant_01")

    unlink = await _unlink(client, corpus_id, doc_id)
    assert unlink.status_code == 200, unlink.text

    # Reload the summary: the manual unlink must stick even though the title
    # still matches the interviewee_id.
    reloaded = await _get_link_summary(client, corpus_id)
    assert reloaded["matched"] == 0
    detail = next(d for d in reloaded["details"] if d["document_id"] == doc_id)
    assert detail["matched"] is False
    assert detail["demographic_row_id"] is None


async def test_manual_reassign_survives_link_summary_reload(client):
    """Reassigning a title-matched row to another transcript must not be undone
    by auto-linking on the next read (which would double-link the row)."""
    corpus_id = await _create_corpus(client, "Reassign Persists")
    await _bulk_ingest(client, corpus_id, ["doc_a", "doc_b"])
    await _upload_and_confirm_csv(client, corpus_id, ["doc_a"])

    summary = await _get_link_summary(client, corpus_id)
    row_id = _row_id(summary, "doc_a")
    doc_a = _doc_id(summary, "doc_a")
    doc_b = _doc_id(summary, "doc_b")

    resp = await _link(client, corpus_id, doc_b, row_id)
    assert resp.status_code == 200, resp.text

    # Reload: the row must remain on doc_b only — doc_a must not be re-linked.
    reloaded = await _get_link_summary(client, corpus_id)
    assert reloaded["matched"] == 1
    assert next(d for d in reloaded["details"] if d["document_id"] == doc_b)["demographic_row_id"] == row_id
    assert next(d for d in reloaded["details"] if d["document_id"] == doc_a)["demographic_row_id"] is None
    # The row maps to exactly one transcript.
    linked_docs = [d for d in reloaded["details"] if d["demographic_row_id"] == row_id]
    assert len(linked_docs) == 1
