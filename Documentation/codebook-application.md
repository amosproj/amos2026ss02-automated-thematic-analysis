# Codebook Application API

"Applying a codebook" means deductive coding: taking an **already-created** codebook (its `Theme`/`Code` rows) and running an LLM pass over selected — or all — transcripts in that codebook's corpus, producing per-document theme presence and span-level code assignments. No new themes or codes are created; this is distinct from [codebook-generation.md](codebook-generation), which synthesizes a codebook from scratch (and can optionally run this same application step at the end).

In the frontend this is the **Analysis** page/blueprint ("Trigger Analysis").

## Base routes

- `POST /codebooks/{codebook_id}/apply-jobs`
- `GET /codebooks/apply-jobs/{job_id}`
- `POST /codebooks/apply-jobs/{job_id}/cancel`
- `GET /codebooks/{codebook_id}/application-runs`
- `GET /codebook-application-runs/{run_id}`
- `DELETE /codebook-application-runs/{run_id}`
- `GET /codebook-application-runs/{run_id}/documents`
- `GET /codebook-application-runs/{run_id}/export`

All JSON responses use the shared envelope `{"success": true, "data": ..., "error": null, "meta": null}`. The export endpoint returns raw CSV instead.

## 1) Create an application job

### `POST /codebooks/{codebook_id}/apply-jobs`

Request body:

- `name` (`string`, optional) — auto-generated as `"<Corpus name> Analysis"` (or `"...Analysis N"` for the Nth run) if omitted
- `custom_id` (`string`, optional) — auto-generated as `RUN-001`, `RUN-002`, ... if omitted
- `transcript_document_ids` (`uuid[]`, optional) — selected transcripts; omit or pass empty to apply to every document in the codebook's corpus
- `corpus_id` (`uuid`, optional, **deprecated**) — the corpus is resolved from the codebook; if supplied it must match

Validates that every `transcript_document_ids` entry actually belongs to the codebook's corpus (`422` otherwise).

Success: `202 Accepted`, `data` is a `CodebookApplicationJobSchema`.

## 2) Job polling

### `GET /codebooks/apply-jobs/{job_id}`

Returns:

- `status`: `queued | running | succeeded | failed | cancelled`
- `phase`: e.g. `loading_codebook`, `applying_codebook` (per-document progress), `persisting`, or terminal
- `progress_percent`: 0-100 estimate
- `application_run_id` (set once the run is created)
- `documents_total`, `documents_done`, `documents_coded`, `documents_failed`
- `llm_tokens_input`, `llm_tokens_output` (nullable) — running LLM token usage, updated live (drives the wait page's live token counter)
- `error_message` (set on failure)

## 3) Job cancellation

### `POST /codebooks/apply-jobs/{job_id}/cancel`

Same semantics as codebook generation jobs: queued jobs cancel immediately; running jobs cancel once observed; terminal jobs return `422`.

## 4) Application runs

An application run (`CodebookApplicationRun`) is the persisted result of one job — one autonomous application of a codebook to a selected transcript set.

### `GET /codebooks/{codebook_id}/application-runs`

Lists all runs for a codebook, newest first, each including its `transcript_document_ids`.

### `GET /codebook-application-runs/{run_id}`

Returns run detail (including final `llm_tokens_input`/`llm_tokens_output` totals) plus every `document_codings` entry, each with nested `theme_assignments` and `code_assignments`.

- `ThemeAssignmentSchema`: `theme_id`, `is_present`, `confidence`, `quote`, `start_char`/`end_char`, `quote_match_status`
- `CodeAssignmentSchema`: `code_id`, `theme_id` (nullable), `quote`, `start_char`/`end_char`, `quote_match_status`, `confidence`, `rationale`

`quote_match_status` is one of `exact`, `normalized`, `fuzzy`, or `not_found` — see [Backend-Software-Architecture-and-Data-Model-Documentation.md, §5 Traceability](Backend-Software-Architecture-and-Data-Model-Documentation).

### `GET /codebook-application-runs/{run_id}/documents`

Same `document_codings` list as above, without the run summary wrapper.

### `DELETE /codebook-application-runs/{run_id}`

Hard-deletes a run and cascades to its document codings and their theme/code assignments. Refused (`422`) while `status == "running"` — cancel the job first.

## 5) Export

### `GET /codebook-application-runs/{run_id}/export?format=theme-based|participant-based`

Returns a raw CSV download (not the JSON envelope).

- `theme-based` — one row per tagged quote (theme, code, quote, confidence, document).
- `participant-based` — the same data with demographic columns repeated per quote, for participant-level analysis.

## Guarded deletion

Deleting a document, codebook, or corpus that has an **active** (`queued`/`running`) application job is refused with `409 Conflict` unless the request passes `force=true`, in which case the affected jobs are cancelled first (`AnalysisDependencyGuard`). Completed/failed/cancelled application runs do not block deletion by themselves — but note that deleting a codebook or document also cascades to any completed runs and codings that reference it.
