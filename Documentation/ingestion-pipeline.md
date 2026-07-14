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
- Messages are ordered by `message_index` before joining.
- Both `human_response` and `chatbot_response` events with non-blank content are kept and formatted as a dialogue: `chatbot_response` -> `Interviewer: ...`, `human_response` -> `Interviewee: ...`.
- Participants with no non-blank `human_response` turns are skipped entirely (a participant who never actually answered contributes no document, even if the chatbot spoke).

For **`.txt`**, **`.docx`**, and **`.pdf`** files, the extracted text is stored directly as one `CorpusDocument` per uploaded file.

Documents can also be submitted directly via the bulk JSON endpoint without a file upload.

## API Endpoints

All routes are under `/api/v1/ingestion`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/corpora` | Create a corpus |
| `GET` | `/corpora` | List corpora (filter by `project_id`) |
| `GET` | `/corpora/{corpus_id}` | Get a single corpus |
| `DELETE` | `/corpora/{corpus_id}` | Delete a corpus (query `force: bool`) |
| `POST` | `/corpora/{corpus_id}/documents/bulk` | Ingest documents from JSON body |
| `POST` | `/corpora/{corpus_id}/documents/copy` | Copy documents from this corpus into an existing target corpus (body: `target_corpus_id`, `document_ids`) |
| `POST` | `/corpora/{corpus_id}/create-corpus-from-documents` | Atomically create a **new** corpus and copy selected documents into it (body: `name`, `document_ids`); the copy is rolled back if corpus creation fails and vice versa |
| `POST` | `/corpora/{corpus_id}/upload` | Ingest from a `.txt`, `.docx`, `.pdf`, or `.jsonl` file upload |
| `GET` | `/corpora/{corpus_id}/documents` | List documents (paginated) |
| `GET` | `/corpora/{corpus_id}/documents/{document_id}` | Get one document's full content and demographic data |
| `DELETE` | `/corpora/{corpus_id}/documents/{document_id}` | Delete one document (query `force: bool`) |

All responses are wrapped in `{"success": true, "data": ...}`. Paginated responses include a `meta` object with `total`, `page`, `page_size`, and `pages`. `copy_documents` and `create_corpus_from_documents` return an `IngestResultSchema`-shaped result — the latter (`CreateCorpusFromDocumentsResultSchema`) additionally nests the created `corpus`, plus `documents_created` and `missing_document_ids` counts for source ids that no longer existed.

Deleting a corpus or document is refused with `409 Conflict` if it has an active (queued/running) codebook application job, unless `force=true` is passed — see [codebook-application.md](codebook-application).

## Notes

- Documents with empty text are silently skipped during ingestion.
- The full document text is stored in `CorpusDocument.content`. There is no separate chunk/passage table — codebook generation and application work directly against each document's full text at analysis time.
- `app/services/langchain_export.py::load_corpus_documents_as_langchain_documents` converts every `CorpusDocument` in a corpus into a `langchain_core.documents.Document` (metadata: `corpus_id`, `document_id`) for ad hoc LangChain tooling. It is not wired into any API route.
