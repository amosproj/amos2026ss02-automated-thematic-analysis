# Codebook Generation API

This document describes backend endpoints for generating a **new** codebook from corpus transcripts using the traceable pipeline (`TraceableAnalysisService`), both synchronous and asynchronous. For applying an **existing** codebook to transcripts (deductive coding, no new codes/themes), see [codebook-application.md](codebook-application).

## Base routes

- `POST /codebooks/generate`
- `POST /codebooks/generate-jobs`
- `GET /codebooks/generate-jobs`
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

## Request body (shared by both sync and async generation)

- `codebook_name` (`string`, required)
- `corpus_id` (`uuid`, required)
- `transcript_document_ids` (`uuid[]`, optional) — if provided, generation uses exactly those corpus documents; if omitted/empty, generation uses all documents in the corpus
- `transcript_sample_size` (`int`, optional, `> 0`) — randomly sample this many transcripts from the corpus instead of using every transcript, to reduce token usage on large corpora. Mutually exclusive with `transcript_document_ids` (to use specific transcripts, create a corpus containing only them)
- `analysis_name` (`string`, optional) — name for the application run created once generation finishes and the codebook is applied
- `custom_id` (`string`, optional) — external identifier for that application run
- `research_query` (`string`, optional, 10-500 chars) — free-text research question that steers extraction/synthesis toward a specific focus
- `researcher_topics` (`string`, optional, up to 500 chars) — comma-separated topics the researcher wants actively surfaced
- `max_refinement_rounds` (`int`, optional, default `5`, range 0-10) — maximum reviewer/refine iterations before the pipeline selects its best codebook candidate. The wizard's "max iterations" dropdown shows a 1-based total iteration count and submits `max_refinement_rounds = iterations - 1`
- `apply_after_generation` (`bool`, optional, default `true`) — whether to run the final deductive-coding pass over all documents in the same job, producing a `CodebookApplicationRun`

## 1) Synchronous generation

### `POST /codebooks/generate`

Runs the full traceable pipeline and persists a codebook in a single request/response cycle. Do not use for large corpora — generation can take minutes; prefer the async job endpoint below for anything beyond a quick demo/small corpus.

Uses the app's currently active LLM provider (see [Backend-Routes settings endpoint](Backend-Software-Architecture-and-Data-Model-Documentation)).

Success — `201 Created`, `data` is a `GeneratedCodebookResponse`:

- `codebook` — id/corpus/version/name/description/research_query/researcher_topics/token-usage metadata
- `application_run_id` (nullable) — set when `apply_after_generation` produced a run
- `transcripts_processed`, `passages_processed`, `themes_created`, `codes_created`
- `documents_coded`, `documents_failed`, `quotes_created` (nullable — set when the codebook was applied)
- `provenance` (object) and `action_log` (array) — structured audit trail of every pipeline decision (extraction, consolidation, synthesis, reviewer actions, final polish, application), for reproducibility/explainability
- `passages_failed`, `failed_passages` — per-passage extraction failures, if any

## 2) Asynchronous job creation

### `POST /codebooks/generate-jobs`

Creates a background `CodebookGenerationJob` and returns immediately (`202 Accepted`). Same request body and selection semantics as the synchronous endpoint.

## 3) Job listing

### `GET /codebooks/generate-jobs`

Query params: `corpus_id` (required), `status` (optional, comma-separated, e.g. `queued,running`).

Returns generation jobs for a corpus, newest first.

## 4) Job polling

### `GET /codebooks/generate-jobs/{job_id}`

Returns the full `CodebookGenerationJobSchema`, including:

- `status`: `queued | running | succeeded | failed | cancelled`
- `phase`: pipeline stage, one of `queued`, `extracting_quote_codes`, `consolidating_codes`, `synthesizing_themes`, `evaluating_iterations`, `persisting_codebook`, `applying_codebook`, or a terminal phase
- `progress_percent`: a 0-100 estimate derived from `phase` and unit counters
- `codebook_id` (set once the codebook is persisted), `application_run_id` (set once applied)
- progress counters: `documents_total/done`, `analysis_units_total/done`, `passages_total/done` (legacy alias), `transcripts_processed`, `passages_processed`, `quotes_created`, `themes_created`, `codes_created`, `documents_coded`, `documents_failed`
- `llm_tokens_input`, `llm_tokens_output` (nullable) — running LLM token usage for the job, updated live as the pipeline progresses (drives the progress page's live token counter)
- `error_message` (set when `failed`)
- `provenance_json` / `action_log_json` — same audit trail as the synchronous response, JSON-encoded

## 5) Job cancellation

### `POST /codebooks/generate-jobs/{job_id}/cancel`

Requests cancellation for a queued/running job.

Behavior:

- queued job: immediately transitions to `cancelled`
- running job: `cancel_requested=true`, final status becomes `cancelled` as soon as the pipeline observes it
- terminal jobs (`succeeded|failed|cancelled`) return `422`

## Pipeline overview

The traceable pipeline (`TraceableAnalysisService.run_analysis`) that both endpoints delegate to:

1. Splits documents into a training set and a held-out evaluation set (`TRACEABLE_HELDOUT_RATIO`).
2. Extracts quote-grounded codes per document via the LLM.
3. Consolidates near-duplicate codes using embedding similarity (`RemoteEmbeddingClient`) plus LLM pairwise/batch relationship classification (equivalent / subordinate / orthogonal).
4. Synthesizes consolidated codes into subthemes and themes.
5. Iteratively evaluates the candidate codebook against held-out documents, asks an LLM reviewer for merge/split/delete/generate/move actions, and refines — up to `max_refinement_rounds` — stopping early once the codebook stabilizes.
6. Applies a final label/description polish pass.
7. Persists the codebook (`Codebook`, `Theme`, `Code`, and their relationship rows).
8. If `apply_after_generation` is true, performs the final deductive-coding pass over **all** documents (not just the training set) and persists a `CodebookApplicationRun` — the same application path used by [codebook-application.md](codebook-application).
