Build, user, and technical documentation
Software architecture description

Theme and code model reference:
- `theme-data-type.md` (Theme/Code fields, hierarchy relationships, and constraints)
- `theme-graph.md` (how the theme/code tree is built and validated)
- includes the theme-frequency list row shape returned by
  `GET /codebooks/{codebook_id}/themes`

Ingestion pipeline reference:
- `ingestion-pipeline.md` (data structures, upload formats, API endpoints)

Demographic import pipeline reference:
- `demographic-pipeline.md` (data structures, CSV validation rules, preview/confirm flow, linking, API endpoints)

Codebook generation reference:
- `codebook-generation.md` (generating a **new** codebook: sync + async endpoints, job lifecycle, traceable pipeline)

Codebook application reference:
- `codebook-application.md` (deductively applying an **existing** codebook to transcripts: apply-jobs, application runs, export)

CSV codebook format reference:
- `csv-codebook-standard.md` (uploadable codebook CSV format and how it maps to Theme/Code)
