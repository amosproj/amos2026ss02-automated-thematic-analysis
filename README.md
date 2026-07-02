# Automated Thematic Analysis (AMOS SS 2026)

## Overview

The **Automated Thematic Analysis** project is designed to automate the extraction and thematic analysis of knowledge from source documents (such as interview transcripts and unstructured text). It provides a robust pipeline that integrates structural text processing with semantic analysis using Large Language Models (LLMs) to build a queryable thematic knowledge graph.

By mapping documents and extracted text chunks to a hierarchical thematic tree (Codebook), the system enables structured qualitative analysis, thematic frequency tracking, and deeper insights into your data corpora.

### Demographic Breakdown

When demographic data (a CSV linked to your transcripts) is uploaded for a corpus, the Theme Browser lets researchers break a theme's frequency down by demographic group:

- **Choose variables per theme** — pick one or more demographic dimensions (e.g. gender, age group, political affiliation) from the variables present in the uploaded data. Only variables that exist in the data are offered, and the selection applies to the selected theme only (changeable at any time). If no demographic data is uploaded, the panel is shown in a disabled state with an explanation.
- **See frequency across groups** — for each selected dimension, the theme's frequency is shown per group as both an absolute count and a percentage within that group, rendered as a table and bar chart. Groups with no occurrences are shown with a count of 0, very small groups are flagged with a small-sample warning, and interviews without a linked demographic value are bucketed as "Not specified". The breakdown updates immediately as dimensions are toggled.

## Built With

- **Backend Framework:** FastAPI (Python 3.11+)
- **Database:** PostgreSQL 16 & SQLAlchemy 2.x (Async)
- **Data Validation & Parsing:** Pydantic v2
- **AI & NLP Pipeline:** LangChain (supports models like Qwen, Mixtral, and Gemma4)
- **Containerization:** Docker & Docker Compose

## Getting Started

### Prerequisites

- Docker and Docker Compose
- (Optional) Python 3.11+ and `uv` / `pip` for local native development

### Installation and Usage

The recommended way to start the application is via Docker.

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd amos2026ss02-automated-thematic-analysis
   ```

2. **Run the bootstrap script:**

   **Linux / macOS / Git Bash (Windows):**
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

   **Windows (PowerShell / Windows Terminal):**
   ```powershell
   .\setup.ps1
   ```

   The script will:
   - Verify Docker and Docker Compose v2 are installed and running
   - Create `Backend/.env` from the template (if it does not exist yet)
   - Build the Docker images and start the stack
   - Wait until the API health check passes, then print the service URLs

3. **Set your LLM API key** (required for analysis features):

   Open `Backend/.env` and replace the placeholder value for `LLM_API_KEY`.

4. **Access the API:**

   - **API Server:** [http://localhost:8000](http://localhost:8000)
   - **Interactive API Docs (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
   - The application also serves a Demo UI and Codebook Selection templates to visually interact with the thematic graph.

### Database Migrations for Existing Environments

If you are updating an existing environment with a preexisting database, you must run the manual database migrations script to add any newly introduced schema columns.

To apply schema migrations, run:
```bash
docker compose run --rm api uv run scripts/alter_db.py
```
*(If you are setting up the project for the first time, this is not needed since the backend automatically provisions a fresh schema on startup).*

#### Common commands

| Task | Linux/macOS | Windows PowerShell |
|------|-------------|-------------------|
| Start stack | `./setup.sh` | `.\setup.ps1` |
| Start (foreground logs) | `./setup.sh -f` | `.\setup.ps1 -Foreground` |
| Run tests | `./setup.sh --test` | `.\setup.ps1 -Test` |
| Stop stack | `./teardown.sh` (or `./setup.sh --down`) | `.\teardown.ps1` (or `.\setup.ps1 -Down`) |
| Stop + delete data | `./setup.sh --down-volumes` | `.\setup.ps1 -DownVolumes` |
| Fully clean restart (destroy data) | `./setup.sh --down-volumes -y && ./setup.sh` | `.\setup.ps1 -DownVolumes -Yes; .\setup.ps1` |

Run `./setup.sh --help` or `Get-Help .\setup.ps1` for the full option reference. Note that `./teardown.sh` is just a convenience wrapper for `./setup.sh --down`, so you do not need to run both.

<details>
<summary>Manual setup (fallback)</summary>

```bash
cd Backend
cp .env.example .env
# Edit .env with your credentials
docker compose up --build
```

</details>

## Further Documentation

For more in-depth technical details, please explore our comprehensive documentation. The **[Backend README](./Backend/README.md)** provides a detailed guide on the FastAPI application structure, API response formats, and instructions for running local development servers or executing test suites. Additionally, the **[Documentation Folder](./Documentation/)** contains deep dives into the system's architecture, including the Ingestion Pipeline, internal data structures (Corpus, Document, Chunk), and the LangChain LLM infrastructure. It also covers our model selection strategies and details on running analysis on the FAU GPU Cluster versus Academic Cloud, ensuring you have all the context needed to extend or deploy the platform.
