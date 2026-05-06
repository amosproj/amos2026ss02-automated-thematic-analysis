# Automated Thematic Analysis (AMOS SS 2026)

## Overview

The **Automated Thematic Analysis** project is designed to automate the extraction and thematic analysis of knowledge from source documents (such as interview transcripts and unstructured text). It provides a robust pipeline that integrates structural text processing with semantic analysis using Large Language Models (LLMs) to build a queryable thematic knowledge graph.

By mapping documents and extracted text chunks to a hierarchical thematic tree (Codebook), the system enables structured qualitative analysis, thematic frequency tracking, and deeper insights into your data corpora.

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

2. **Set up configuration:**
   Navigate to the `Backend` folder and prepare your environment variables.

   ```bash
   cd Backend
   cp .env.example .env
   # Update .env if necessary with specific credentials
   ```

3. **Start the application:**
   Launch the FastAPI backend and the PostgreSQL database container.

   ```bash
   docker compose up --build
   ```

4. **Access the API:**

   - **API Server:** [http://localhost:8000](http://localhost:8000)
   - **Interactive API Docs (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
   - The application also serves a Demo UI and Codebook Selection templates to visually interact with the thematic graph.

## Further Documentation

For more in-depth technical details, please explore our comprehensive documentation. The **[Backend README](./Backend/README.md)** provides a detailed guide on the FastAPI application structure, API response formats, and instructions for running local development servers or executing test suites. Additionally, the **[Documentation Folder](./Documentation/)** contains deep dives into the system's architecture, including the Ingestion Pipeline, internal data structures (Corpus, Document, Chunk), and the LangChain LLM infrastructure. It also covers our model selection strategies and details on running analysis on the FAU GPU Cluster versus Academic Cloud, ensuring you have all the context needed to extend or deploy the platform.
