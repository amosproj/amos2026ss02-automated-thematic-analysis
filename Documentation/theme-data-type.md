# Theme and Code Data Types

This backend supports thematic analysis through explicitly separated `Theme` and `Code` entities. This separation ensures that high-level thematic structures remain distinct from specific, granular analytical codes.

## Entities

### Theme (`themes`)
Core fields:
- `id` (`uuid`, primary key; `sqlalchemy.Uuid`)
- `codebook_id` (`uuid`, FK → `codebooks.id`, CASCADE DELETE)
- `label` (`string`, indexed; unique within codebook via `uq_theme_codebook_label`)
- `description` (`string`, nullable)
- `is_active` (`boolean`, default `true`)

### Code (`codes`)
Core fields:
- `id` (`uuid`, primary key; `sqlalchemy.Uuid`)
- `codebook_id` (`uuid`, FK → `codebooks.id`, CASCADE DELETE)
- `label` (`string`, indexed; unique within codebook via `uq_code_codebook_label`)
- `description` (`string`, nullable)
- `is_active` (`boolean`, default `true`)

### Codebook (`codebooks`)
Core fields:
- `id` (`uuid`, primary key; `sqlalchemy.Uuid`)
- `corpus_id` (`uuid`, FK → `corpora.id`, CASCADE DELETE)
- `name` (`string`)
- `description` (`string`, nullable)
- `version` (`int`)
- `created_by` (`string`)

## UUID Column Type

All UUID columns use `sqlalchemy.Uuid(as_uuid=True)` — the generic, dialect-agnostic UUID type. This ensures compatibility with both PostgreSQL (production) and SQLite (testing). Do **not** use `sqlalchemy.dialects.postgresql.UUID`.

## Relationship Constraints

### Codebook Memberships
Both Themes and Codes have active membership junction tables linking them to Codebooks:
- `codebook_theme_relationships` (1 codebook : n Themes)
- `codebook_code_relationships` (1 codebook : n Codes)

### Hierarchical Relationships (DAG)
The system uses Directed Acyclic Graph (DAG) structures to model relationships:
- `theme_hierarchy_relationships`: Maps `Theme` → `Theme` relationships (i.e. Subthemes nested under Themes or other Subthemes).
- `theme_code_relationships`: Maps `Theme` → `Code` relationships, allowing Codes to be structurally nested under Themes.

## Notes

- This hybrid design ensures `Theme` and `Code` remain isolated at the database level while seamlessly combining into unified hierarchy trees for frontend rendering and analysis.
- A Subtheme is simply a Theme with a parent — there is no separate `Subtheme` database table. Whether a Theme is a root theme or subtheme is derived from the presence/absence of a parent in `theme_hierarchy_relationships`.
- Subtheme nesting is arbitrary depth: a SUBTHEME may have another SUBTHEME as a parent, forming multi-level hierarchies.

## Response Schema: Codebook Detail

`GET /codebooks/{codebook_id}` and `POST /codebooks/` return a `CodebookDetailSchema`:

```json
{
  "id": "<uuid>",
  "corpus_id": "<uuid>",
  "name": "My Codebook",
  "description": null,
  "version": 1,
  "created_by": "researcher",
  "themes": [
    {
      "id": "<uuid>",
      "node_type": "THEME",
      "name": "Remote work reshapes daily life",
      "description": "...",
      "children": [
        {
          "id": "<uuid>",
          "node_type": "SUBTHEME",
          "name": "Work-life boundary challenges",
          "description": "...",
          "children": [
            {
              "id": "<uuid>",
              "node_type": "CODE",
              "name": "Difficulty separating work and home life",
              "description": "..."
            }
          ]
        }
      ]
    }
  ],
  "codes": [
    {
      "id": "<uuid>",
      "node_type": "CODE",
      "name": "Root-level orphan code",
      "description": "..."
    }
  ]
}
```

The `themes` array contains only root `Theme` nodes (those without a parent). Each node recursively embeds its `children`, which may be `SUBTHEME` or `CODE` nodes. The `codes` array contains only `Code` nodes that have no parent theme (root-level orphan codes).

## Frontend Statistics

The frontend calculates theme statistics (root themes, total themes, sub-themes) by filtering out Code nodes. Only nodes where `node_type !== 'CODE'` are counted in theme metrics. Codes have their own separate counter.

## API Endpoints

- `GET /codebooks/{codebook_id}`
  - Returns `CodebookDetailSchema`: the full nested theme+code tree and the flat list of root-level orphan codes (see schema above).
- `POST /codebooks/`
  - Creates a new codebook from a `CodebookCreateRequest` and returns `CodebookDetailSchema`.
- `GET /codebooks/{codebook_id}/themes`
  - Returns a flat list of all active `Theme` nodes (excludes `Code` nodes).
  - Each item contains `theme_name`, `occurrence_count`, and `interview_coverage_percentage`.
  - `occurrence_count` and `interview_coverage_percentage` are currently placeholder `0` values.
- `GET /codebooks/{codebook_id}/themes/tree`
  - Returns the recursive theme tree for the selected codebook (excludes `Code` nodes).
  - Optional query param: `root_theme_id` to return only one subtree.