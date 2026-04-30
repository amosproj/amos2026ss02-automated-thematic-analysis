# Codebook Data Model

## Purpose

A `Codebook` is the version boundary for deductive coding. One row represents one concrete codebook state.

## Core Fields

- `id` (`codebook_id`): unique identifier of a concrete codebook row.
- `project_id`: stable grouping key for a codebook lineage.
- `version`: integer version within one `project_id`.
- `previous_version_id`: optional link to the previous codebook row.
- `name`, `description`, `research_question`: business metadata.
- `status`: lifecycle state (for example `DRAFT`).
- `created_by`: actor that created the row.

## Constraints and Integrity

- `(project_id, version)` is unique.
- `previous_version_id` references `codebooks.id` (`SET NULL` on delete).

## Runtime Usage

- Read endpoints resolve codebooks by `project_id` plus optional `version`.
- If `version` is omitted, highest version in the project is selected.
- Downstream theme/code queries are executed in the resolved `codebook_id` scope.

## Practical Semantics

- `project_id` identifies the lineage.
- `codebook_id` identifies one concrete snapshot.
- New version => new `codebook_id`; linkage to prior row is optional but supported.

## Source References

- `Backend/app/models/codebook.py`
- `Backend/app/services/theme_read.py`
- `Backend/app/routers/themes.py`
