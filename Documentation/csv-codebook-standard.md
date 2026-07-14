# CSV Codebook Standard

The automated thematic analysis platform supports uploading predefined codebooks via CSV files. 

## File Format

- **Encoding**: UTF-8 (BOM is supported and safely ignored).
- **Separator**: Comma (`,`) separated values.

## Required Columns

The CSV must contain the following column headers. The headers are case-insensitive and surrounding whitespace is ignored:

- `Node Type`
- `Name`
- `Description`
- `Parent Name`

## Rules and Constraints

1. **Hierarchy Limits**: A single codebook must contain between 1 and 50 nodes.
2. **Name**: Cannot be empty. Each name should ideally be unique within the codebook to allow reliable hierarchical referencing.
3. **Node Type**: Must be exactly one of the following values (case-insensitive during upload):
   - `THEME`
   - `SUBTHEME`
   - `CODE`
4. **Parent Name**: 
   - If the `Node Type` is `THEME`, the `Parent Name` **must be empty**.
   - If the `Node Type` is `SUBTHEME` or `CODE`, the `Parent Name` **must be provided** and must exactly match the `Name` of an existing node defined earlier in the CSV file.
5. **Description**: Can be empty.

## Relationship to System Entities

As noted in the [theme-data-type.md](theme-data-type) documentation, `Theme` and `Code` are separate, first-class persisted entities. CSV codebook uploads preserve this distinction:

- `THEME` and `SUBTHEME` rows are persisted as `Theme` objects, connected to each other via `ThemeHierarchyRelationship` edges derived from the `Parent Name` column.
- `CODE` rows are persisted as `Code` objects (their own `codes` table), attached to their parent `Theme`/`Subtheme` via `ThemeCodeRelationship` edges. A `CODE` row's `Parent Name` must therefore reference a `THEME` or `SUBTHEME` row, never another `CODE` row — codes are always leaves.

The node type from the CSV **is** preserved, in the sense that it determines which table the row lands in (`themes` vs. `codes`); it is not stored as a literal column on either table.

## Example

```csv
Node Type,Name,Description,Parent Name
THEME,Remote work reshapes daily life,Broad changes caused by remote work,
SUBTHEME,Work-life boundary challenges,Difficulty separating work and personal life,Remote work reshapes daily life
CODE,Difficulty separating work and home life,Specific struggles to detach from work,Work-life boundary challenges
```
