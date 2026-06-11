# Codebook Application API

This document describes autonomous application of an existing codebook to corpus transcripts.

## Core behavior

- Each application creates a new `CodebookApplicationRun`.
- Existing runs are never overwritten.
- A run can target either selected transcript IDs or all transcripts in the corpus.
- Each transcript receives one `DocumentCoding` row.
- LLM failures are retried up to 3 times per transcript.
- If one transcript still fails after retries, only that transcript is marked failed and the job continues.
- Generated coding assignments are span-oriented so future manual coding can reuse the same structures.

## Job endpoints

### `POST /api/v1/codebooks/{codebook_id}/apply-jobs`

Creates an asynchronous codebook application job.

Request body:

```json
{
  "corpus_id": "uuid",
  "transcript_document_ids": ["uuid"]
}
```

Selection behavior:

- `transcript_document_ids` present: apply to exactly those corpus documents.
- `transcript_document_ids` omitted or empty: apply to all corpus documents.

Success:

- `202 Accepted`
- Returns a `CodebookApplicationJobSchema` snapshot.

### `GET /api/v1/codebooks/apply-jobs/{job_id}`

Polls an application job.

Important fields:

- `status`: `queued | running | succeeded | failed | cancelled`
- `phase`: `queued | loading_codebook | coding_documents | persisting | succeeded | failed | cancelled`
- `progress_percent`
- `documents_total`
- `documents_done`
- `documents_coded`
- `documents_failed`
- `application_run_id`
- `error_message`

Partial transcript failures are reported as structured JSON in `error_message` while the job can still finish with `status = succeeded`.

### `POST /api/v1/codebooks/apply-jobs/{job_id}/cancel`

Requests cancellation for a queued or running application job.

Queued jobs cancel immediately. Running jobs cancel when the worker observes the cancellation flag between transcript-level operations.

## Run endpoints

### `GET /api/v1/codebooks/{codebook_id}/application-runs`

Lists historical application runs for a codebook, newest first.

### `GET /api/v1/codebook-application-runs/{run_id}`

Returns a run and its document codings.

### `GET /api/v1/codebook-application-runs/{run_id}/documents`

Returns document codings for one run, including theme and code assignments.

## Quote tagging

Every code assignment stores:

- `quote`
- `start_char`
- `end_char`
- `quote_match_status`

Matching order:

1. `exact`: exact substring match.
2. `normalized`: whitespace-normalized match.
3. `fuzzy`: strict fuzzy fallback.
4. `not_found`: quote persisted, but no reliable span found.

Manual coding can later reuse `DocumentCoding`, `ThemeAssignment`, and `CodeAssignment` rather than introducing a separate incompatible model.

## Theme frequencies

`GET /api/v1/codebooks/{codebook_id}/themes` now accepts optional `application_run_id`.

- If provided, frequencies are computed for that run.
- If omitted, frequencies use the latest successful application run.
- If no successful run exists, existing behavior is preserved: all frequencies are zero.

