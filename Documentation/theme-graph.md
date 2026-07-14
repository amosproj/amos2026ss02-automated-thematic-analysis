# Theme Graph (Tree-First)

## Purpose

`ThemeGraphService` builds a complete, unbalanced tree of themes and codes from database state for one selected `codebook_id`.

The implementation is tree-first and uses `anytree` for runtime materialization and cycle detection.

## Minimal Data Model

The service reads from five tables:

- `themes`:
  - `id`, `label`, `is_active`
- `codes`:
  - `id`, `label`, `is_active`
- `codebook_theme_relationships`:
  - active membership of themes in a codebook (`codebook_id`, `theme_id`, `is_active`)
- `codebook_code_relationships`:
  - active membership of codes in a codebook (`codebook_id`, `code_id`, `is_active`)
- `theme_hierarchy_relationships`:
  - active parent-child edges between themes (`codebook_id`, `parent_theme_id`, `child_theme_id`, `is_active`)
- `theme_code_relationships`:
  - active edges attaching a code to its parent theme (`codebook_id`, `theme_id`, `code_id`, `is_active`)

## Behavior

1. Resolve active theme and code nodes for the given `codebook_id`.
2. Resolve active theme-hierarchy edges (theme -> theme) and theme-code edges (theme -> code), where all endpoints are in the active node set.
3. Materialize the runtime tree with `anytree`, with codes always as leaves.
4. Return either:
   - the full forest (all roots), or
   - a subtree when `root_theme_id` is provided.

## Validation Rules

- A theme cannot be parent of itself.
- A child (theme or code) can have at most one active parent.
- Cycles are invalid and rejected.
- Codes are always leaves — a code cannot be the parent of a theme or another code.

## Public API (Service Layer)

- `validate_theme_dag(codebook_id)`:
  Returns `is_valid` + violation messages.
- `build_theme_dag(codebook_id)`:
  Returns nodes, edges, and computed root ids.
- `get_theme_tree(codebook_id, root_theme_id=None)`:
  Returns nested `ThemeTreeNode` structures (themes with nested subthemes/codes).

## API Endpoints

- `GET /codebooks/{codebook_id}/themes`
  - Returns a flat list of all active codebook themes with real occurrence/coverage stats.
  - Each list item contains `theme_name`, `occurrence_count`, and `interview_coverage_percentage`, computed from the codebook's latest succeeded application run (or a specific run via `application_run_id`).

- `GET /codebooks/{codebook_id}/themes/tree`
  - Returns the theme + code tree for the selected codebook.
  - Optional query param: `root_theme_id`.

See [theme-data-type.md](theme-data-type) for the full field-level model and [codebook-application.md](codebook-application) for how application runs (the source of frequency/quote data) are produced.
