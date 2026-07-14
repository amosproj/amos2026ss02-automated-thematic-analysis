# Demographic Import Pipeline

Handles uploading, validating, previewing, and persisting interviewee demographic data per corpus.

## Data Structures

### DemographicFiles (`demographic_files`)

One confirmed demographic import batch for a corpus.

- `id` (`uuid`, primary key)
- `name` (`string`, max 255) - logical import name, unique within one corpus
- `corpus_id` (`uuid`, FK -> `corpora.id`, CASCADE DELETE)
- `original_columns` (`json`) - header columns as uploaded
- `created_at`, `updated_at` (`timestamp`)
- Unique constraint on `(corpus_id, name)`

### DemographicRow (`demographic_row`)

One parsed row from a confirmed import file.

- `id` (`uuid`, primary key)
- `demographic_file_id` (`uuid`, FK -> `demographic_files.id`, CASCADE DELETE)
- `corpus_id` (`uuid`, FK -> `corpora.id`, CASCADE DELETE)
- `row_number` (`int`) - 1-based line position within the imported data rows
- `interviewee_id` (`string`) - parsed from CSV `username` column
- `data` (`json`) - dynamic demographic key/value pairs (all non-`username` columns)
- Unique constraint on `(corpus_id, interviewee_id)`

## Upload and Confirmation Flow

1. `POST /upload`
   - validates extension, size, UTF-8 decode, CSV shape, and uniqueness constraints
   - writes a pending `.csv` file and metadata to uploads storage
   - returns a preview (`rows_detected`, `columns_detected`, `sample_rows`) and `import_id`
2. `POST /confirm`
   - with `confirm=true`: revalidates and persists `DemographicFiles` + `DemographicRow` records
   - with `confirm=false`: cancels and deletes the pending files

Pending uploads expire based on `DEMOGRAPHIC_UPLOAD_TTL_SECONDS`.

## CSV Validation Rules

- Supported extension: `.csv`
- UTF-8 (including UTF-8 BOM) is required
- Delimiter is auto-detected between comma (`,`) and semicolon (`;`) via `csv.Sniffer`
- Header row is required
- Header must contain a `username` column
- At least one demographic column in addition to `username` is required
- Data rows must exist
- Rows with extra columns are rejected as malformed
- Empty `username` values are rejected
- Duplicate `username` values in one upload are rejected
- `username` values already present in the same corpus are rejected
- Empty demographic cells are allowed and stored as empty strings

## API Endpoints

All routes are under `/api/v1/demographic/{corpus_id}`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/upload` | Validate CSV, stage pending upload, return preview + `import_id` |
| `POST` | `/confirm?import_id=...&confirm=true|false` | Persist or cancel a pending upload |
| `GET` | `/files` | List confirmed demographic imports (paginated) |
| `GET` | `/dimensions` | List demographic variable names available for theme breakdowns (excludes `username`; empty if nothing uploaded) |
| `GET` | `/rows` | List confirmed demographic rows (paginated, optional `demographic_file_id` filter) |
| `DELETE` | `/files/{file_id}` | Delete a demographic file and all of its rows |
| `GET` | `/link-summary` | Transcript <-> demographic-row linking summary for the corpus |
| `PUT` | `/documents/{document_id}/link` | Manually set/reassign the demographic row linked to one transcript (body: `{"demographic_row_id": "<uuid>"}`); a row maps to at most one transcript, so linking an already-linked row moves it |
| `DELETE` | `/documents/{document_id}/link` | Remove the demographic link from one transcript |

`PUT`/`DELETE .../link` both return the refreshed `LinkingSummary`. Auto-linking (matching document title against `interviewee_id`, case/whitespace-insensitive) runs automatically on document ingestion and on demographic confirm — `GET /link-summary` deliberately does not re-run it, so it doesn't silently revert a manual unlink.

All responses use the shared response envelope shape.
