# Theme Data Model

## Purpose

`Theme` rows represent thematic artifacts (theme/subtheme nodes). They are reused through relationship tables rather than being duplicated per codebook by default.

## Core Entities

- `themes`:
  - `id` (`theme_id`), `label`, `description`, `level`, `status`, `created_by`.
- `codebook_theme_relationships`:
  - membership edge (`codebook_id` -> `theme_id`), typed as `CONTAINS`.
- `theme_relationships`:
  - theme-to-theme edge inside a codebook scope (`CHILD_OF`, `RELATED_TO`, `EQUIVALENT_TO`).
- `code_theme_relationships`:
  - code-to-theme edge in codebook scope.

## Scoping Model

- Theme rows are global artifacts.
- "Theme in a codebook version" is defined by an active membership edge in `codebook_theme_relationships`.
- Structural and semantic edges are always scoped by `codebook_id`.

## Lifecycle Behavior

- Node lifecycle is on `Theme.status` (for example `ACTIVE`, `CANDIDATE`, `DEPRECATED`, `MERGED`, `DELETED`).
- Edge lifecycle is on relationship status (`ACTIVE`/`REMOVED`), preserving history.
- Deletions are usually soft in graph operations (membership/edges deactivated).

## Versioning Implication

- `theme_id` may be reused across multiple codebook versions.
- Reusing a theme does not automatically reuse edges; edges are version-scoped via `codebook_id`.
- Updating shared theme fields affects all versions where that `theme_id` is used.

## Source References

- `Backend/app/models/themes.py`
- `Backend/app/services/theme_graph.py`
- `Backend/app/services/theme_read.py`
