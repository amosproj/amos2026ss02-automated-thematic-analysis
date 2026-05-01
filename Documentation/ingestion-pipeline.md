# Ingestion Pipeline

Handles uploading interview data into the system, chunking it for analysis, and exposing it to downstream components.

## Data Structures

### Corpus (`corpora`)

Top-level container grouping documents for one analytical project.

- `id` (`uuid`, primary key)
- `project_id` (`uuid`, indexed) — references the owning project (JUST PLACEHOLDER FOR NOW. Project structure not set up yet)
- `name` (`string`)
- `created_at`, `updated_at` (`timestamp`)

### CorpusDocument (`corpus_documents`)

One source document within a corpus. Stores only metadata — the actual text lives in chunks.

- `id` (`uuid`, primary key)
- `corpus_id` (`uuid`, FK -> `corpora.id`, CASCADE DELETE)
- `title` (`string`)
- `created_at`, `updated_at` (`timestamp`)

### CorpusChunk (`corpus_chunks`)

A fixed-size word-window slice of a document. This is the unit consumed by thematic analysis.

- `id` (`uuid`, primary key)
- `document_id` (`uuid`, FK -> `corpus_documents.id`, CASCADE DELETE)
- `chunk_index` (`int`) — zero-based position within the parent document
- `text` (`text`)
- `created_at`, `updated_at` (`timestamp`)
- Unique constraint on `(document_id, chunk_index)`

## Chunking

Text is split into overlapping word windows:

- `chunk_size_words` — maximum words per chunk (configured via `INGESTION_CHUNK_SIZE_WORDS`)
- `overlap_words` — words shared between consecutive chunks (configured via `INGESTION_CHUNK_OVERLAP_WORDS`)
- `stride = chunk_size_words - overlap_words`

## Supported Upload Format

Only `.jsonl` files are accepted via the upload endpoint. Each line must be a JSON object representing one message from a chatbot interview session:

```json
{"username": "p001", "event_type": "human_response", "message_index": 2, "message_content": "I feel..."}
```

- Messages are grouped by `username` — each participant becomes one `CorpusDocument`.
- Only `human_response` events are included in the document text; chatbot turns are excluded.
- Messages are ordered by `message_index` before joining.
- Participants with no non-blank human responses are skipped.

Documents can also be submitted directly via the bulk JSON endpoint without a file upload.

## API Endpoints

All routes are under `/api/v1/ingestion`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/corpora` | Create a corpus |
| `GET` | `/corpora` | List corpora (filter by `project_id`) |
| `GET` | `/corpora/{corpus_id}` | Get a single corpus |
| `POST` | `/corpora/{corpus_id}/documents/bulk` | Ingest documents from JSON body |
| `POST` | `/corpora/{corpus_id}/upload` | Ingest from a `.jsonl` file upload |
| `GET` | `/corpora/{corpus_id}/documents` | List documents (paginated) |
| `GET` | `/corpora/{corpus_id}/chunks` | List chunks (paginated, filterable by `document_id`) |

All responses are wrapped in `{"success": true, "data": ...}`. Paginated responses include a `meta` object with `total`, `page`, `page_size`, and `pages`.

## Notes

- Documents with empty text are silently skipped during ingestion.
- The full document text is not stored — only the chunks. Reconstruct the original by joining chunks in `chunk_index` order.
- Chunks are exposed to LangChain consumers via `load_corpus_chunks_as_langchain_documents`, which returns `langchain_core.documents.Document` objects with `corpus_id`, `document_id`, `chunk_id`, and `chunk_index` in metadata.
