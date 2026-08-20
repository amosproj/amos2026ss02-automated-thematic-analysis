# Researcher Trial: Second Local Generation Algorithm

Date: 2026-08-20

## Scope

I simulated a researcher adding a second local codebook-generation algorithm by
reading the repository README files, creating a new algorithm under
`Backend/research_algorithms`, and validating it through the Docker test
workflow.

## Algorithm Created

- File: `Backend/research_algorithms/keyword_frequency.py`
- Config value:

  ```text
  GENERATION_ALGORITHM=research_algorithms.keyword_frequency:create
  ```

- Behavior: derives a small deterministic codebook from document-frequency
  keyword terms, prioritizing terms from `research_query` and
  `researcher_topics` before other transcript terms.
- LLM use: none. The algorithm sets `requires_llm = False`.

## Verification

Commands run from the repository root:

```bash
docker compose run --rm api-test pytest tests/test_generation_plugins.py
docker compose run --rm api-test ruff check research_algorithms tests/test_generation_plugins.py
docker compose run --rm api-test pytest -q
docker compose run --rm api-test pytest tests/test_codebook_application_jobs_api.py::test_apply_codebook_job_persists_coding_and_quote_spans -q
```

Results:

- `tests/test_generation_plugins.py`: 18 passed.
- `ruff check research_algorithms tests/test_generation_plugins.py`: all checks
  passed.
- Full backend suite: 478 passed, 1 skipped, 1 xpassed, 1 failed. The failure
  was `test_apply_codebook_job_persists_coding_and_quote_spans`, where an async
  application job remained at `running` / `persisting` until the test timeout.
- Isolated rerun of that failing test: 1 passed.

## Issues Fixed During Trial

1. There was only one researcher-facing example algorithm, and it returned a
   static placeholder codebook. I added `keyword_frequency.py` as a second,
   content-derived example.
2. There was no test coverage for a real algorithm in
   `Backend/research_algorithms`. I added coverage for loading, validation,
   progress reporting, and persistence through `CodebookGenerationService`.
3. My first ranking implementation treated research-query terms as an unordered
   set. Docker tests exposed that tied terms were sorted alphabetically instead
   of by researcher prompt order. I fixed the example to preserve first
   occurrence from `research_query` / `researcher_topics`.
4. `Backend/research_algorithms/README.md` did not list available examples or
   provide a direct Docker command to validate algorithm plugins. I added both.

## Remaining Shortcomings

2. The local algorithm README gives a Unix `cp` command but no PowerShell
   equivalent, even though the top-level setup instructions explicitly support
   Windows PowerShell.
3. "Run the normal codebook-generation workflow" is underspecified. The docs do
   not provide a minimal API or UI walkthrough for generating a codebook with
   `apply_after_generation=false`, which is useful for non-LLM algorithms.
4. The Docker instructions say to restart the API container but do not give the
   exact command or clarify whether commands are expected from the repository
   root compose file or `Backend/docker-compose.yml`.
5. The `GenerationAlgorithm` protocol does not declare `requires_llm`, while the
   routers inspect it dynamically with `getattr(..., True)`. This works at
   runtime, but the public contract is weaker than the examples imply.
6. In this PowerShell trial, UTF-8 punctuation in README output rendered as
   mojibake in the terminal. This may be a local console encoding issue, but it
   makes terminal-based documentation reading less clear on Windows.
7. The full backend Docker suite exposed a transient async-job timing failure in
   `test_apply_codebook_job_persists_coding_and_quote_spans`. The same test
   passed when rerun in isolation, so it appears unrelated to the second
   algorithm but is still a researcher-visible reliability issue.

## Suggested Follow-Up

Add a short "Try a local algorithm end to end" section that includes:

- PowerShell and Unix copy commands.
- `GENERATION_ALGORITHM=research_algorithms.keyword_frequency:create`.
- `docker compose restart api`.
- A minimal API or UI path with `apply_after_generation=false`.
- The focused Docker test command for researchers who want to validate their
  algorithm before running it through the UI.
