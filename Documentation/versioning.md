# Versioning for Codebooks, Themes, and Links

## Why Versioning Is Needed

The two referenced papers describe iterative qualitative analysis where schemas/themes are refined over rounds and then reused deductively on later data:

- Auto-TA emphasizes iterative theme refinement rounds (`Theta(t)`) and consensus-based theme evolution.
- LOGOS emphasizes iterative open -> axial -> selective coding with a reusable, persistent structure and train-derived codebook reuse on test data.

In practice, this requires immutable historical snapshots for reproducibility and separate linkage scopes per refinement step.

Sources:
- https://arxiv.org/pdf/2506.23998
- https://arxiv.org/pdf/2509.24294

## Current Setup (Implemented)

## 1) Codebook lineage and version boundary

- `codebooks` is the version boundary.
- `project_id` groups one lineage.
- `version` is the round/snapshot number inside a lineage.
- `(project_id, version)` is unique (`uq_codebook_project_version`).
- `previous_version_id` links to the prior snapshot row.

This maps well to iterative refinement and reproducible readbacks.

## 2) Theme and code reuse with version-scoped links

- Themes and codes are global entities (`themes`, `codes`).
- Membership into a specific codebook version is represented by relationship tables.
- Structure and semantics are version-scoped via `codebook_id` on relationship rows:
  - `codebook_theme_relationships`
  - `theme_relationships`
  - `code_theme_relationships`
  - `codebook_code_relationships`
  - `code_relationships`

This means the same `theme_id` can appear in multiple codebook versions, while hierarchy/semantic links are still isolated by version.

## Illustrative Examples

Example 1: Same project, two codebook versions

- `project_id = "acme_support"`
- version 1: `codebook_id = cb_v1`
- version 2: `codebook_id = cb_v2`, `previous_version_id = cb_v1`

This is valid because `(project_id, version)` is unique and both versions belong to one lineage.

Example 2: Reusing one theme across versions

- `theme_id = th_latency` is linked to both `cb_v1` and `cb_v2` via `codebook_theme_relationships`.
- In `cb_v1`: `CHILD_OF(th_latency -> th_performance)`
- In `cb_v2`: `CHILD_OF(th_latency -> th_reliability)`

This is valid and expected: the same theme can exist in multiple versions, and its parent can differ per version because edges are scoped by `codebook_id`.
