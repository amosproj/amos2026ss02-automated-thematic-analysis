import pytest
from flask import session

def test_analysis_index_no_corpus(client, fake_backend):
    # Setup state to simulate backend failure which results in no active corpus
    fake_backend.raise_on = "list_corpora"
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = None
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    assert b"No active corpus selected." in resp.data
    assert b"disabled style=\"pointer-events: none;\"" in resp.data

def test_analysis_index_no_transcripts(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [] # No documents
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    assert b"Corpus has no transcripts." in resp.data
    assert b"disabled style=\"pointer-events: none;\"" in resp.data

def test_analysis_index_no_codebook(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [] # No codebook
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    assert b"Please configure a codebook for this corpus." in resp.data
    assert b"disabled style=\"pointer-events: none;\"" in resp.data

def test_analysis_index_ready(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    # Button should NOT be disabled
    assert b"disabled style=\"pointer-events: none;\"" not in resp.data

def test_trigger_analysis_success(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.post("/analysis/trigger", data={
        "corpus_id": corpus_id,
        "codebook_id": "cb1"
    })
    
    assert resp.status_code == 302
    assert "/analysis/job/" in resp.headers["Location"]

def test_trigger_analysis_missing_data(client, fake_backend):
    resp = client.post("/analysis/trigger", data={
        "corpus_id": "test-corpus",
        # missing codebook_id
    })
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/analysis/")
    
    follow = client.get(resp.headers["Location"])
    assert b"Missing corpus or codebook ID." in follow.data

def test_analysis_wait_page(client):
    resp = client.get("/analysis/job/test-job")
    assert resp.status_code == 200
    assert b"Applying Codebook" in resp.data

def test_analysis_job_status(client, fake_backend):
    job = fake_backend.trigger_analysis("test", "cb1")
    resp = client.get(f"/analysis/job/{job['id']}/status")
    assert resp.status_code == 200
    data = resp.json
    assert data["id"] == job["id"]
    assert data["status"] == "queued"
