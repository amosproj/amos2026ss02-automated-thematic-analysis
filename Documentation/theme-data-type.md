# Theme Data Type and Constraints

This backend supports thematic analysis through `Theme` and its relationships. A `Code` entity also exists in the backend (`codes` table), but note that CSV codebook uploads map all node types (`THEME`, `SUBTHEME`, and `CODE`) to `Theme` objects internally — see [csv-codebook-standard.md](csv-codebook-standard) for details.

## Theme (`themes`)

Core fields:
- `id` (`uuid`, primary key; PostgreSQL `UUID`)
- `codebook_id` (`uuid`, FK -> `codebooks.id`, CASCADE DELETE, indexed)
- `label` (`string`, indexed) — unique within one codebook
- `description` (`text`, optional)
- `is_active` (`boolean`, default `true`)

## Relationship Constraints

### Codebook-Theme Membership (`codebook_theme_relationships`)
1 codebook : n Themes
- `id` (`uuid`, primary key; PostgreSQL `UUID`)
- Foreign keys:
  - `codebook_id` (`uuid`) -> `codebooks.id`
  - `theme_id` (`uuid`) -> `themes.id`
- `is_active` (`boolean`, default `true`)

## Notes

- This is an intentionally simplified design for sprint scope reduction.
- If stricter behavior is needed later (e.g., uniqueness per active membership), add constraints in a follow-up migration.

## API Endpoint

- `GET /codebooks/{codebook_id}/themes`
  - Returns a flat list of all active codebook themes.
  - Each list item contains `theme_name`, `occurrence_count`, and `interview_coverage_percentage`.
  - For now, `occurrence_count` and `interview_coverage_percentage` are placeholder `0` values.
- `GET /codebooks/{codebook_id}/themes/tree`
  - Returns the theme tree for the selected codebook.
  - Optional query param: `root_theme_id`.
