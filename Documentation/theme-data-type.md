# Theme and Code Data Types and Constraints

This backend supports thematic analysis through two distinct persisted entities: `Theme` (themes and subthemes) and `Code` (leaf-level codes), plus the relationship tables that connect them into a hierarchy scoped to one codebook.

`Code` is **not** folded into `Theme` — see [csv-codebook-standard.md](csv-codebook-standard) for how CSV/LLM-generated codebooks route each node type to the correct table.

## Theme (`themes`)

Core fields:
- `id` (`uuid`, primary key; PostgreSQL `UUID`)
- `codebook_id` (`uuid`, FK -> `codebooks.id`, CASCADE DELETE, indexed)
- `label` (`string`, indexed) — unique within one codebook
- `description` (`text`, optional)
- `is_active` (`boolean`, default `true`)

## Code (`codes`)

Core fields:
- `id` (`uuid`, primary key; PostgreSQL `UUID`)
- `codebook_id` (`uuid`, FK -> `codebooks.id`, CASCADE DELETE, indexed)
- `label` (`string`, indexed) — unique within one codebook
- `description` (`text`, optional)
- `is_active` (`boolean`, default `true`)

## Relationship Constraints

### Codebook-Theme Membership (`codebook_theme_relationships`)
1 codebook : n Themes
- `id` (`uuid`, primary key)
- Foreign keys: `codebook_id` -> `codebooks.id`, `theme_id` -> `themes.id`
- `is_active` (`boolean`, default `true`)

### Codebook-Code Membership (`codebook_code_relationships`)
1 codebook : n Codes
- `id` (`uuid`, primary key)
- Foreign keys: `codebook_id` -> `codebooks.id`, `code_id` -> `codes.id`
- `is_active` (`boolean`, default `true`)

### Theme Hierarchy (`theme_hierarchy_relationships`)
Directed parent/child edge between two themes, scoped to one codebook.
- `id` (`uuid`, primary key)
- Foreign keys: `codebook_id` -> `codebooks.id`, `parent_theme_id` -> `themes.id`, `child_theme_id` -> `themes.id`
- `is_active` (`boolean`, default `true`)

### Theme-Code Attachment (`theme_code_relationships`)
Directed edge attaching a code to its parent theme (the code is always a leaf), scoped to one codebook.
- `id` (`uuid`, primary key)
- Foreign keys: `codebook_id` -> `codebooks.id`, `theme_id` -> `themes.id`, `code_id` -> `codes.id`
- `is_active` (`boolean`, default `true`)

## Notes

- This is an intentionally simplified design for sprint scope reduction; `Codebook.version` is a plain incrementing integer, not true multi-version coexistence.
- If stricter behavior is needed later (e.g., uniqueness per active membership), add constraints in a follow-up migration.

## API Endpoints

- `GET /codebooks/{codebook_id}/themes`
  - Returns a flat list of all active codebook themes with real occurrence/coverage stats (not placeholders).
  - Each list item contains `theme_name`, `occurrence_count`, and `interview_coverage_percentage`, computed against one application run.
  - Optional query param `application_run_id` — if omitted, defaults to the codebook's latest **succeeded** application run.
- `GET /codebooks/{codebook_id}/themes/tree`
  - Returns the combined theme + code tree for the selected codebook (themes/subthemes as internal nodes, codes as leaves).
  - Optional query param: `root_theme_id`.
- `GET /codebooks/{codebook_id}/themes/{theme_id}/quotes`
  - Paginated, confidence-ranked list of quotes assigned to one theme within an application run (query params `page`, `page_size`, `application_run_id`).
- `GET /codebooks/{codebook_id}/themes/{theme_id}/demographic-breakdown`
  - Breaks a theme's presence down by one or more demographic dimensions (query params `dimensions` comma-separated, `application_run_id`).

See [codebook-application.md](codebook-application) for how application runs are created and what "applying a codebook" means.
