# Local Codebook Generation Algorithms

This directory is for local Python algorithms that replace only codebook generation.
The backend still owns document loading, request validation, jobs, cancellation,
progress persistence, database transactions, draft validation, codebook
application, API responses, and provenance wrapping.

## Create An Algorithm

1. Copy `example.py`:

   ```bash
   cp Backend/research_algorithms/example.py Backend/research_algorithms/my_algorithm.py
   ```

   PowerShell:

   ```powershell
   Copy-Item Backend\research_algorithms\example.py Backend\research_algorithms\my_algorithm.py
   ```

2. Edit only the `generate()` method and, if useful, `algorithm_id` and
   `algorithm_version`. Set `requires_llm = False` for algorithms that do not
   call an LLM. Set it to `True` when the algorithm needs the configured chat or
   embedding provider.

3. Make it available to the selector. The shortest route is to configure it as
   the server default in `Backend/.env`:

   ```text
   GENERATION_ALGORITHM=research_algorithms.my_algorithm:create
   ```

   When this path is not one of the built-in entries, it appears in the Home-page
   selector as **Custom Algorithm**. To expose several custom algorithms with
   descriptive names, add an `AlgorithmSpec` for each one to
   `Backend/app/generation/registry.py` instead.

4. Recreate the backend container, select the algorithm on the Home page, and
   run the normal codebook-generation workflow. The selected algorithm is saved
   as an application-wide default and is captured when each job is created.

The built-in default is:

```text
GENERATION_ALGORITHM=app.generation.algorithms.traceable:create
```

## Included Examples

- `example.py`: minimal static template for copying into a new file.
- `keyword_frequency.py`: deterministic baseline that derives up to five codes
  from frequent non-stopword transcript terms. It does not require an LLM and
  is available as **Keyword Frequency (Demo)** in the Home-page selector.

## Run Locally

From `Backend/`, start the API as usual:

```bash
uvicorn app.main:app --reload
```

Python can import `research_algorithms.*` because the backend root is on the
module path during local execution.

## Run With Docker

The Docker image copies this directory. The development Compose files also mount
`research_algorithms` read-only into the API container, so editing an algorithm
does not require rebuilding the image. Recreate the API container after changing
`GENERATION_ALGORITHM` so Compose reloads `Backend/.env`. Recreating also picks up
algorithm code changes reliably.

Run Docker commands from the repository root when using the root
`docker-compose.yml`:

```bash
docker compose up -d --force-recreate api
```

Alternatively, stop and start the stack while preserving database data:

```bash
./teardown.sh
./setup.sh
```

If you intentionally work from `Backend/` with `Backend/docker-compose.yml`, use
the same command from that directory instead.

To run the algorithm/plugin checks from the repository root:

```bash
docker compose run --rm api-test pytest tests/test_generation_plugins.py
docker compose run --rm api-test ruff check research_algorithms tests/test_generation_plugins.py
```

## Try A Local Algorithm End To End

This path validates non-LLM generation without applying the codebook to
transcripts afterward.

1. Select **Keyword Frequency (Demo)** on the Home page. For an API-only trial,
   make the equivalent settings request:

   ```bash
   curl -X PUT http://localhost:8000/api/v1/settings/generation-algorithm \
     -H "Content-Type: application/json" \
     -d '{"algorithm":"keyword_frequency_example"}'
   ```

2. Create a small corpus:

   ```bash
   curl -X POST http://localhost:8000/api/v1/ingestion/corpora \
     -H "Content-Type: application/json" \
     -d '{"corpus_id":"11111111-1111-1111-1111-111111111111","name":"Local Algorithm Trial"}'
   ```

3. Add transcripts:

   ```bash
   curl -X POST http://localhost:8000/api/v1/ingestion/corpora/11111111-1111-1111-1111-111111111111/documents/bulk \
     -H "Content-Type: application/json" \
     -d '{"documents":[{"title":"Transcript 1","text":"Manual handoffs create delays and unclear ownership."},{"title":"Transcript 2","text":"Operational handoffs need clearer ownership and trust."}]}'
   ```

4. Generate a codebook without final application:

   ```bash
   curl -X POST http://localhost:8000/api/v1/codebooks/generate \
     -H "Content-Type: application/json" \
     -d '{"codebook_name":"Keyword Frequency Trial","corpus_id":"11111111-1111-1111-1111-111111111111","research_query":"handoffs and delays","researcher_topics":"operations, ownership","apply_after_generation":false}'
   ```

The response should include `themes_created`, `codes_created`, and provenance
showing `research_algorithms.keyword_frequency:create`.

You can run the same flow through the UI at http://localhost:3000 by uploading
transcripts, generating a codebook, and leaving final application disabled for a
non-LLM trial.

## Input And Output

`generate()` receives:

- `GenerationInput.documents`: immutable `GenerationDocument` items with
  `document_id`, `title`, and `content`.
- `GenerationInput.research_query` and `researcher_topics`.
- `GenerationInput.random_seed` for reproducible randomized choices.
- `GenerationInput.algorithm_options`, currently including
  `max_refinement_rounds`.
- `GenerationContext`, including the selected LLM provider/model, progress and
  phase callbacks, and a cancellation check.

`generate()` must return `GenerationResult` with a `CodebookDraft`.
Algorithm objects must also expose `algorithm_id`, `algorithm_version`, and
`requires_llm`.

## Validation Rules

The backend rejects invalid drafts before persistence. A valid draft must have:

- at least one theme and one code;
- non-empty unique theme keys and code keys;
- non-empty labels, max 255 characters;
- no duplicate normalized theme or code labels;
- valid parent theme references;
- no self-parent links or hierarchy cycles;
- codes that either omit `theme_key` or reference a known theme key;
- JSON-serializable provenance, action log, and token usage metadata;
- bounded output size.

If validation fails, the generation job fails cleanly and no partial codebook is
committed.

## Reproducibility Metadata

The backend stores provenance for jobs with:

- configured `module:factory` specification;
- `algorithm_id` and `algorithm_version`;
- SHA-256 hash of the algorithm source file when available;
- random seed;
- selected document IDs;
- selected LLM provider, chat model, and embedding model when available;
- token usage;
- algorithm options.

Algorithms can add their own JSON-serializable provenance under
`GenerationResult.provenance`.

## Troubleshooting

- `must use 'module.path:factory' format`: set `GENERATION_ALGORITHM` to a
  Python module and a factory, separated by one colon.
- `module ... could not be imported`: check the file name, package path, and
  that the backend was restarted.
- `has no factory`: define a `create()` function in the module.
- `must provide an async callable generate()`: `generate` must be declared with
  `async def`.
- `invalid codebook draft`: inspect the named theme/code in the error and fix
  duplicate keys, empty labels, unknown parents, cycles, or malformed metadata.
