# Ingestion Pipeline

Handles uploading interview data into the system and exposing it to downstream components such as codebook generation.

## Data Structures

### Corpus (`corpora`)

Top-level container grouping documents for one analytical project.

- `id` (`uuid`, primary key)
- `project_id` (`uuid`, indexed) — references the owning project (placeholder; full project structure not yet implemented)
- `name` (`string`)
- `created_at`, `updated_at` (`timestamp`)

### CorpusDocument (`corpus_documents`)

One source document within a corpus. Stores both metadata and the full document text.

- `id` (`uuid`, primary key)
- `corpus_id` (`uuid`, FK -> `corpora.id`, CASCADE DELETE)
- `demographic_row_id` (`uuid | null`, FK -> `demographic_row.id`, SET NULL) — linked interviewee demographics, if available
- `title` (`string`)
- `filename` (`string | null`) — original uploaded filename after collision resolution; null for body-ingested docs
- `content` (`text`) — full text of the document
- `created_at`, `updated_at` (`timestamp`)

## Supported Upload Formats

The upload endpoint accepts `.txt`, `.docx`, `.pdf`, and `.jsonl` files.

For **`.jsonl`** files, each line must be a JSON object representing one message from a chatbot interview session:

```json
{"username": "p001", "event_type": "human_response", "message_index": 2, "message_content": "I feel..."}
```

- Messages are grouped by `username` — each participant becomes one `CorpusDocument`.
- Only `human_response` events are included in the document text; chatbot turns are excluded.
- Messages are ordered by `message_index` before joining.
- Participants with no non-blank human responses are skipped.

For **`.txt`**, **`.docx`**, and **`.pdf`** files, the extracted text is stored directly as one `CorpusDocument` per uploaded file.

Documents can also be submitted directly via the bulk JSON endpoint without a file upload.

## API Endpoints

All routes are under `/api/v1/ingestion`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/corpora` | Create a corpus |
| `GET` | `/corpora` | List corpora (filter by `project_id`) |
| `GET` | `/corpora/{corpus_id}` | Get a single corpus |
| `DELETE` | `/corpora/{corpus_id}` | Delete a corpus |
| `POST` | `/corpora/{corpus_id}/documents/bulk` | Ingest documents from JSON body |
| `POST` | `/corpora/{corpus_id}/upload` | Ingest from a `.txt`, `.docx`, `.pdf`, or `.jsonl` file upload |
| `GET` | `/corpora/{corpus_id}/documents` | List documents (paginated) |

All responses are wrapped in `{"success": true, "data": ...}`. Paginated responses include a `meta` object with `total`, `page`, `page_size`, and `pages`.

## Notes

- Documents with empty text are silently skipped during ingestion.
- The full document text is stored in `CorpusDocument.content`. Codebook generation splits the content into in-memory passages at analysis time — passages are not persisted as separate database rows.
- Chunks are exposed to LangChain consumers via `load_corpus_chunks_as_langchain_documents`, which returns `langchain_core.documents.Document` objects with `corpus_id`, `document_id`, and `chunk_index` in metadata.
