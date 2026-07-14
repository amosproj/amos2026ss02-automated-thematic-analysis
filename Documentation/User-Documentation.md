# User Documentation

Welcome to the **Automated Thematic Analysis** project! This system automates qualitative coding of interview transcripts: it extracts grounded codes and themes with an LLM, lets you review and adjust them, and then applies the resulting codebook back onto your transcripts so you can explore frequency, coverage, and demographic breakdowns.

## Getting Started

The platform is used through the Flask web UI. Every action is scoped to a **Corpus** — a named collection of transcripts (a single default workspace corpus is created for you automatically, or you can create additional ones from the corpus selector on the Uploads page). You can also spin off a new corpus from an existing one by selecting a subset of transcripts on the Transcripts list page and using "Create New Corpus" — handy for narrowing a large corpus down before generating a codebook.

### Accessing the Platform

- **Web UI:** [http://localhost:3000](http://localhost:3000)
- **API Server & Endpoints:** [http://localhost:8000](http://localhost:8000)
- **Interactive API Docs (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Core Features

### 1. Upload

The Uploads page hosts three side-by-side entry points for one corpus:

- **Transcripts** — `.jsonl` interview files. Each file becomes one transcript (a `.jsonl` file is split into one transcript per participant `username`).
- **Codebook (CSV)** — a predefined hierarchy of themes/subthemes/codes; see [csv-codebook-standard.md](csv-codebook-standard) for the exact format. Uploading opens a review editor where you can rename, reparent, or delete nodes before saving.
- **Demographic data (CSV)** — one row per participant (matched to transcripts by participant name), used later to break theme results down by demographic group. Demographic uploads are staged as a preview first; you explicitly confirm or discard before anything is saved. If a demographic row's participant name doesn't automatically match a transcript title, you can link them manually on the **Linking** board.

### 2. Codebook Creation

A **Codebook** is the hierarchy of themes, subthemes, and codes you'll apply to your transcripts. You get to it via **Codebooks -> New Codebook**, which offers three modes:

- **Fully automatic** — the LLM extracts quote-grounded codes from your transcripts, consolidates and synthesizes them into themes, iteratively reviews itself against held-out transcripts, and (by default) immediately applies the finished codebook to all documents. You only provide a name and, optionally, a research question or topics of interest to steer it.
- **Semi-automatic** — same LLM generation, but you land on a review editor afterward to adjust the result before saving.
- **Manual / CSV upload** — bring your own codebook via the CSV format above, or build one from scratch in the review editor.

Generation runs as a background job with a live progress page; you can cancel it while it's running.

### 3. Applying a Codebook (Analysis)

Applying a codebook means running deductive LLM coding: taking an existing codebook and tagging which themes/codes are present in selected (or all) transcripts, with an exact supporting quote for every tag. This is done from the **Analysis** page ("Trigger Analysis"): pick a codebook and the transcripts to run it against, then submit. Each run is saved as an **application run** so you can revisit, export, or delete it later, and re-apply the same codebook to new transcripts without regenerating it.

### 4. Exploring Results

- **Theme Browser** (**Codebooks -> a codebook -> Themes**): a frequency table (occurrence count + interview coverage %), the theme/code hierarchy tree, and a details panel showing supporting quotes and a demographic breakdown for the selected theme. You can switch which application run's results are shown.
- **Read Transcript** (**Transcripts -> a transcript -> Read**): the full transcript text with speaker turns highlighted; open it from a specific application run to also see exactly which spans were tagged with which theme/code.
- **Export**: download a codebook's structure as CSV from the theme browser, or export one or more application runs' coded results as CSV (theme-based or participant-based shape) from the Analysis page.

---

## Example Workflow

1. **Setup:** Ensure an LLM provider API key is configured (see [Build & Deploy](Build-&-Deploy)); pick the active provider from the home page.
<img width="634" height="514" alt="image" src="https://github.com/user-attachments/assets/f0d4192a-dfc4-47f4-88bb-84ef199494f5" />

2. **Upload:** Go to Uploads, create or select a corpus, and add your transcript files (and, optionally, demographic data).
<img width="1129" height="432" alt="image" src="https://github.com/user-attachments/assets/84a595ed-da5d-4768-adc0-8b9962c5a9df" />

3. **Generate or upload a codebook:** Use **New Codebook** for LLM generation, or upload a CSV codebook.
<img width="504" height="778" alt="image" src="https://github.com/user-attachments/assets/f929d36f-758b-4b7a-864d-bbdab872303a" />

4. **Apply it:** Go to **Analysis**, select the codebook and transcripts, and trigger the run (skip this if you used "Fully automatic," which already applied it).

5. **Review:** Explore the Theme Browser for frequencies and quotes, use Read Transcript to trace individual coded spans back to the source text, and export results when you're ready to share them.
<img width="585" height="613" alt="image" src="https://github.com/user-attachments/assets/2aa16035-e52a-45da-914a-a154d7c4d4c2" />

