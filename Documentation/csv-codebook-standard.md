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

As noted in the `theme-data-type.md` documentation, dedicated code-related backend tables were intentionally removed to simplify the system design. To still fully support the hierarchical nature of qualitative thematic analysis, the system uses a unified `Theme` entity structure. 

All nodes—whether defined as `THEME`, `SUBTHEME`, or `CODE` in the CSV—are stored internally as `Theme` objects in the database, with their respective `NodeType` attribute preserved. The parent-child relationships defined via the `Parent Name` column are mapped using `ThemeHierarchyRelationship` edges.

## Example

```csv
Node Type,Name,Description,Parent Name
THEME,Remote work reshapes daily life,Broad changes caused by remote work,
SUBTHEME,Work-life boundary challenges,Difficulty separating work and personal life,Remote work reshapes daily life
CODE,Difficulty separating work and home life,Specific struggles to detach from work,Work-life boundary challenges
```
