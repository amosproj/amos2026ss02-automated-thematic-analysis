"""Tests for the manual transcript ↔ demographic linking board and proxy routes.

Routes are corpus-scoped under the demographic blueprint:
  GET  /demographic/<corpus_id>/linking          -> board page
  POST /demographic/<corpus_id>/linking/link      -> JSON link/reassign
  POST /demographic/<corpus_id>/linking/unlink    -> JSON unlink

The `fake_backend` fixture (conftest.py) stands in for the real BackendClient.
"""

from web.services.backend_client import BackendNotFoundError, BackendValidationError

CORPUS = "test-corpus-id"

DOC_A = "doc-aaaa"
DOC_B = "doc-bbbb"
ROW_1 = "row-1111"
ROW_2 = "row-2222"


def _summary(matched=1):
    """A summary with DOC_A linked to ROW_1 and DOC_B unlinked; ROW_2 free."""
    return {
        "total_transcripts": 2,
        "matched": matched,
        "details": [
            {"document_id": DOC_A, "document_title": "Alpha", "demographic_row_id": ROW_1, "matched": True},
            {"document_id": DOC_B, "document_title": "Beta", "demographic_row_id": None, "matched": False},
        ],
        "demographic_rows": [
            {"row_id": ROW_1, "interviewee_id": "alpha", "data": {"age": "29"},
             "linked_document_id": DOC_A, "linked": True},
            {"row_id": ROW_2, "interviewee_id": "beta", "data": {"age": "34"},
             "linked_document_id": None, "linked": False},
        ],
    }


# ---- Board page -------------------------------------------------------------


def test_linking_board_renders(client, fake_backend):
    fake_backend.demographic_link_summary = _summary()

    resp = client.get(f"/demographic/{CORPUS}/linking", follow_redirects=True)

    assert resp.status_code == 200
    assert b"Link Transcripts" in resp.data
    assert b"data-linking-board" in resp.data
    assert b"data-transcripts-column" in resp.data
    assert b"data-demographic-column" in resp.data
    assert b"linking_board.js" in resp.data
    # Embedded state for the JS to render from.
    assert b'id="linking-data"' in resp.data
    assert b"Alpha" in resp.data
    assert b"alpha" in resp.data
    # The two proxy URLs are wired onto the board element.
    assert f"/demographic/{CORPUS}/linking/link".encode() in resp.data
    assert f"/demographic/{CORPUS}/linking/unlink".encode() in resp.data


def test_linking_board_backend_error(client, fake_backend):
    fake_backend.raise_on = "get_demographic_link_summary"
    resp = client.get(f"/demographic/{CORPUS}/linking", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Couldn't load linking data right now." in resp.data
    assert b"Traceback" not in resp.data


# ---- Link route -------------------------------------------------------------


def test_link_route_success(client, fake_backend):
    fake_backend.demographic_link_summary = _summary(matched=2)

    resp = client.post(
        f"/demographic/{CORPUS}/linking/link",
        json={"document_id": DOC_B, "demographic_row_id": ROW_2},
    )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["matched"] == 2
    assert len(body["transcripts"]) == 2
    assert len(body["demographic_rows"]) == 2
    assert fake_backend.last_link_request == {
        "corpus_id": CORPUS,
        "document_id": DOC_B,
        "demographic_row_id": ROW_2,
    }


def test_link_route_missing_fields(client, fake_backend):
    resp = client.post(f"/demographic/{CORPUS}/linking/link", json={"document_id": DOC_B})
    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False
    assert fake_backend.last_link_request is None


def test_link_route_validation_error(client, fake_backend):
    fake_backend.raise_on = ("link_transcript", BackendValidationError)
    resp = client.post(
        f"/demographic/{CORPUS}/linking/link",
        json={"document_id": DOC_B, "demographic_row_id": "bogus"},
    )
    assert resp.status_code == 422
    assert resp.get_json()["ok"] is False


def test_link_route_not_found(client, fake_backend):
    fake_backend.raise_on = ("link_transcript", BackendNotFoundError)
    resp = client.post(
        f"/demographic/{CORPUS}/linking/link",
        json={"document_id": "missing", "demographic_row_id": ROW_2},
    )
    assert resp.status_code == 404
    assert resp.get_json()["ok"] is False


# ---- Unlink route -----------------------------------------------------------


def test_unlink_route_success(client, fake_backend):
    fake_backend.demographic_link_summary = _summary(matched=0)

    resp = client.post(
        f"/demographic/{CORPUS}/linking/unlink",
        json={"document_id": DOC_A},
    )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert fake_backend.last_unlink_request == {"corpus_id": CORPUS, "document_id": DOC_A}


def test_unlink_route_missing_doc(client, fake_backend):
    resp = client.post(f"/demographic/{CORPUS}/linking/unlink", json={})
    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False
    assert fake_backend.last_unlink_request is None


# ---- End-to-end manual override flow (DoD) ----------------------------------


def test_manual_override_link_then_unlink_flow(client, fake_backend):
    """Drive the manual override end-to-end through the Flask routes:
    open the board, link an unmatched transcript, then unlink it again."""
    fake_backend.demographic_link_summary = _summary(matched=1)

    # 1) Board loads with one unlinked transcript flagged.
    board = client.get(f"/demographic/{CORPUS}/linking", follow_redirects=True)
    assert board.status_code == 200

    # 2) Link the unlinked transcript (DOC_B) to the free row (ROW_2).
    fake_backend.demographic_link_summary = _summary(matched=2)
    link = client.post(
        f"/demographic/{CORPUS}/linking/link",
        json={"document_id": DOC_B, "demographic_row_id": ROW_2},
    )
    assert link.status_code == 200
    assert link.get_json()["matched"] == 2
    assert fake_backend.last_link_request["document_id"] == DOC_B

    # 3) Unlink it again.
    fake_backend.demographic_link_summary = _summary(matched=1)
    unlink = client.post(
        f"/demographic/{CORPUS}/linking/unlink",
        json={"document_id": DOC_B},
    )
    assert unlink.status_code == 200
    assert unlink.get_json()["ok"] is True
    assert fake_backend.last_unlink_request["document_id"] == DOC_B
