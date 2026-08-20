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

2. Edit only the `generate()` method and, if useful, `algorithm_id` and
   `algorithm_version`.

3. Select it in `Backend/.env`:

   ```text
   GENERATION_ALGORITHM=research_algorithms.my_algorithm:create
   ```

4. Restart the backend and run the normal codebook-generation workflow.

The built-in default is:

```text
GENERATION_ALGORITHM=app.generation.algorithms.traceable:create
```

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
does not require rebuilding the image. Restart the API container after changing
`GENERATION_ALGORITHM` or algorithm code.

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
