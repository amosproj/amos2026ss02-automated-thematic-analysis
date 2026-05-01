# Theme Data Type and Constraints

This backend currently supports thematic analysis around `Theme` and its relationships.
Code-related entities were intentionally removed.

## Theme (`themes`)

Core fields:
- `id` (`uuid`, primary key; PostgreSQL `UUID`)
- `label` (`string`, indexed)
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
