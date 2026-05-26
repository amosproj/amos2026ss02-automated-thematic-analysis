# Codebook Generation API

This document describes backend endpoints for creating codebooks from corpus transcripts, both synchronous and asynchronous.

## Base routes

- `POST /codebooks/generate`
- `POST /codebooks/generate-jobs`
- `GET /codebooks/generate-jobs/{job_id}`
- `POST /codebooks/generate-jobs/{job_id}/cancel`

All responses use the shared envelope:

```json
{
  "success": true,
  "data": {},
  "error": null,
  "meta": null
}
```

## 1) Synchronous generation

### `POST /codebooks/generate`

Creates and persists a codebook in a single request/response cycle.
Do not use for large corpora or when generation time is expected to exceed a few seconds.
Ideally only for single files. 

Request body:

- `codebook_name` (`string`, required)
- `corpus_id` (`uuid`, required)
- `transcript_document_ids` (`uuid[]`, optional)

Selection behavior:

- If `transcript_document_ids` is provided, generation uses exactly those corpus documents.
- If `transcript_document_ids` is omitted (or empty), generation uses all documents in the corpus.

Success:

- `201 Created`
- `data.codebook` with id/project/version metadata
- generation counters:
  - `transcripts_processed`
  - `passages_processed`
  - `themes_created`
  - `codes_created`

## 2) Asynchronous job creation

### `POST /codebooks/generate-jobs`

Creates a background generation job and returns immediately.

Request body:

- `codebook_name` (`string`, required)
- `corpus_id` (`uuid`, required)
- `transcript_document_ids` (`uuid[]`, optional)

Selection behavior is identical to synchronous mode:

- provided IDs => selected subset
- omitted/empty => all corpus documents

Success:

- `202 Accepted`
- job snapshot in `data` with `status = queued|running`

## 3) Job polling

### `GET /codebooks/generate-jobs/{job_id}`

Returns current job state and progress.

Relevant fields:

- `status`: `queued | running | succeeded | failed | cancelled`
- `passages_done`, `passages_total`
- `codebook_id` (set when `succeeded`)
- `error_message` (set when `failed`)
- `transcripts_processed`, `passages_processed`, `themes_created`, `codes_created` (set when `succeeded`)

## 4) Job cancellation

### `POST /codebooks/generate-jobs/{job_id}/cancel`

Requests cancellation for a queued/running job.

Behavior:

- queued job: immediately transitions to `cancelled`
- running job: `cancel_requested=true`, final status becomes `cancelled` as soon as cancellation is observed
- terminal jobs (`succeeded|failed|cancelled`) return `422`
