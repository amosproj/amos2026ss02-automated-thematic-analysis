# Theme DAG

## Purpose

The theme graph service projects themes into a DAG per `codebook_id` and provides validated hierarchy/tree reads plus refinement operations.

## Graph Definition

- Nodes: themes active in the selected codebook membership scope.
- Hierarchy edges: `CHILD_OF` (`source=child`, `target=parent`).
- Optional semantic edges: `RELATED_TO`, `EQUIVALENT_TO`.
- Active working set defaults to node statuses `{CANDIDATE, ACTIVE}`.

## Invariants

- No self-links.
- `CHILD_OF` enforces single active parent per child.
- Cycles are rejected.
- Tree generation runs validation first and fails on invalid hierarchy.

## Read Projection

1. Resolve node set for one `codebook_id`.
2. Resolve active edges for that same `codebook_id`.
3. Compute roots (nodes without incoming active `CHILD_OF`).
4. Build nested tree recursively (optionally from one `root_theme_id`).

## Mutation Semantics

- Operations (`add`, `move`, `merge`, `split`, `replace`, `delete`) preserve lineage by deactivating old edges and creating new active edges.
- Soft delete keeps historical records; hard delete is blocked when a theme is still active in other codebooks.

## Source References

- `Backend/app/services/theme_graph.py`
- `Backend/app/schemas/theme_graph.py`
