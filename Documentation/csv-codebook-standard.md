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

## Node Types

Three node types are supported, mapping to two distinct backend entities:

| Node Type  | Backend Entity | Behaviour |
|------------|---------------|-----------|
| `THEME`    | `Theme`       | A root-level thematic category. `Parent Name` must be empty. |
| `SUBTHEME` | `Theme`       | A child theme nested under a `THEME` or another `SUBTHEME`. `Parent Name` is required. |
| `CODE`     | `Code`        | A specific analytical code. `Parent Name` is optional; if set, must reference a `THEME` or `SUBTHEME`. |

Both `THEME` and `SUBTHEME` are stored as `Theme` objects in the database — a subtheme is simply a `Theme` that has a parent edge in `theme_hierarchy_relationships`. `CODE` nodes are stored as separate `Code` objects in the `codes` table.

## Rules and Constraints

1. **Node Count**: A codebook must contain between 1 and 50 nodes total (all types combined).
2. **Name**: Cannot be empty. Names must be unique within the codebook to enable reliable parent references.
3. **Node Type**: Must be exactly one of `THEME`, `SUBTHEME`, or `CODE` (case-insensitive during upload).
4. **Parent Name**:
   - `THEME`: `Parent Name` **must be empty** — root themes have no parent.
   - `SUBTHEME`: `Parent Name` **must be provided** and must match the `Name` of an existing `THEME` or `SUBTHEME` node defined in the CSV.
   - `CODE`: `Parent Name` is optional. If provided, it must match the `Name` of a `THEME` or `SUBTHEME` node — **not another `CODE`**. If the referenced parent does not exist or is itself a `CODE`, the link is silently dropped and the code becomes a root-level node. If empty, the code is a standalone root-level node.
5. **Description**: Can be empty.
6. **Node ordering**: CSV row order does **not** affect whether parent references resolve. The parser collects all nodes in a first pass and validates hierarchy references in a second pass, so a node may reference a parent that appears later in the file.

## Relationship to System Entities

The backend explicitly differentiates between structural themes and specific codes to preserve the semantics of qualitative thematic analysis:

- `THEME` and `SUBTHEME` nodes are stored as `Theme` objects in the database. Their parent-child relationships are mapped using `ThemeHierarchyRelationship` edges.
- `CODE` nodes are stored as `Code` objects in their own dedicated table. If a `CODE` specifies a `Parent Name`, it is linked to its parent `Theme` via the `ThemeCodeRelationship` junction table.

This hybrid approach allows the frontend to dynamically reconstruct a unified hierarchical tree using Directed Acyclic Graph (DAG) structures while ensuring that `Theme` and `Code` data remain semantically isolated at the database level.

## Upload API

Two endpoints support CSV-based codebook creation:

- `POST /codebooks/parse-csv` — validates and previews a CSV without persisting anything. Returns the parsed node list on success or a `422` error with a human-readable message on failure.
- `POST /codebooks/` — creates the codebook and persists all nodes atomically. Accepts the same node list as a JSON body (`CodebookCreateRequest`).

## Examples

### Themes only (no codes)

```csv
Node Type,Name,Description,Parent Name
THEME,Safety,Matters related to physical or psychological safety,
THEME,Efficiency,How quickly and effectively tasks are completed,
THEME,Communication,Issues with team communication and transparency,
```

### Root-level code (no parent)

```csv
Node Type,Name,Description,Parent Name
THEME,Technical Issues,Recurring software problems,
CODE,Bug Reports,Software bug occurrences reported by users,
CODE,Unrelated orphan code,A standalone code with no parent,
```

### Full hierarchy — nested subthemes and codes at multiple levels

```csv
Node Type,Name,Description,Parent Name
THEME,Remote work reshapes daily life,Broad changes caused by remote work,
SUBTHEME,Work-life boundary challenges,Difficulty separating work and personal life,Remote work reshapes daily life
CODE,Difficulty separating work and home life,Specific struggles to detach from work,Work-life boundary challenges
CODE,Working longer hours,Working past normal hours,Remote work reshapes daily life
CODE,Family interruptions during meetings,Family members interrupting video calls,Remote work reshapes daily life
THEME,Communication barriers,Issues communicating remotely,
SUBTHEME,Asynchronous communication issues,Delays or misunderstandings in async comms,Communication barriers
CODE,Missed messages,Messages not seen in time,Asynchronous communication issues
CODE,Lack of tone in text,Misinterpretation of text tone,Communication barriers
```

Note that `Working longer hours` and `Family interruptions during meetings` are attached directly to the root `THEME`, while `Difficulty separating work and home life` is attached to the `SUBTHEME` one level down.