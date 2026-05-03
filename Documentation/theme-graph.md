# Theme Graph (Tree-First)

## Purpose

`ThemeGraphService` builds a complete, unbalanced theme tree from database state for one selected `codebook_id`.

The implementation is tree-first and uses `anytree` for runtime materialization and cycle detection.

## Minimal Data Model

The service reads from three tables:

- `themes`:
  - `id`, `label`, `is_active`
- `codebook_theme_relationships`:
  - active membership of themes in a codebook (`codebook_id`, `theme_id`, `is_active`)
- `theme_hierarchy_relationships`:
  - active parent-child edges (`codebook_id`, `parent_theme_id`, `child_theme_id`, `is_active`)

## Behavior

1. Resolve active theme nodes for the given `codebook_id`.
2. Resolve active hierarchy edges where both endpoints are in the active node set.
3. Materialize the runtime tree with `anytree`.
4. Return either:
   - the full forest (all roots), or
   - a subtree when `root_theme_id` is provided.

## Validation Rules

- A theme cannot be parent of itself.
- A child can have at most one active parent.
- Cycles are invalid and rejected.

## Public API (Service Layer)

- `validate_theme_dag(codebook_id)`:
  Returns `is_valid` + violation messages.
- `build_theme_dag(codebook_id)`:
  Returns nodes, edges, and computed root ids.
- `get_theme_tree(codebook_id, root_theme_id=None)`:
  Returns nested `ThemeTreeNode` structures.

## API Endpoint

- `GET /codebooks/{codebook_id}/themes/tree`
  - optional query param: `root_theme_id`
