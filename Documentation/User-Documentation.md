# User Documentation

Welcome to the **Automated Thematic Analysis** project! This system automates the extraction and thematic analysis of knowledge from source documents, allowing you to discover deeper insights into your data corpora.

## Getting Started

The platform allows you to upload documents (such as interview transcripts), map them against a Codebook (a hierarchical thematic tree), and explore the resulting thematic knowledge graph.

### Accessing the Platform
Once the application is running, you can access the following services:
- **API Server & Endpoints:** [http://localhost:8000](http://localhost:8000)
- **Interactive API Docs (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Demo UI:** You can interact visually with the Codebook Selection and explore the thematic graph via the frontend templates.

---

## Core Features

### 1. Corpora Management
A **Corpus** is a collection of related documents.
- **Create a Corpus:** Group your research materials logically (e.g., "Customer Interviews 2026").
- **Upload Documents:** Add unstructured text documents (PDFs, transcripts) to a Corpus. The system automatically breaks these documents down into manageable **Chunks**.
- **Delete Corpus:** Note that deleting a corpus is a permanent action and is ONLY accessible from the "Upload" view of your selected corpus.

### 2. Codebook Interaction
A **Codebook** defines the themes you are looking for. It acts as a hierarchical structure of nodes/themes.
- **Select Codebooks:** Use the UI to explore different Codebook structures.
- **Thematic Mapping:** The AI pipeline uses Large Language Models (LLMs) to automatically map chunks of text from your documents to relevant themes in the Codebook.

### 3. Graph Exploration
The system builds a queryable thematic knowledge graph based on your analysis.
- **Theme Frequency Tracking:** Easily see which themes appear most frequently across your corpus.
- **Traceability:** Click on a mapped theme to view the exact text snippets (chunks) and source documents that generated the match.

### 4. LLM and Embedding Models
The platform uses two kinds of AI models:
- **Large Language Models (LLMs):** Generate codebooks, interpret transcripts, and assign quotes to themes.
- **Embedding models:** Convert short texts into numeric vectors so the system can compare meaning. They are used behind the scenes to detect similar or duplicate codes before the LLM performs more detailed checks.

The Home page **LLM Provider** setting controls both model types. If the active provider is changed, new codebook generation and analysis jobs use the newly selected provider. Jobs that are already running keep the provider they started with.

Administrators can configure the concrete embedding model in `Backend/.env`:
- `EMBEDDING_MODEL_FAU` is used with the `FAU` provider.
- `EMBEDDING_MODEL` is used with the `ACADEMIC` provider.

Commercial OpenAI-compatible providers can be used by setting the selected provider's base URL, API key, chat model, and embedding model in `Backend/.env`. For example, OpenAI can be configured through the `ACADEMIC` slot with `LLM_BASE_URL=https://api.openai.com/v1`, `LLM_API_KEY=<your_openai_api_key>`, `LLM_MODEL=<chat_model>`, and `EMBEDDING_MODEL=text-embedding-3-large`. This sends both LLM and embedding requests for new jobs to that configured provider.

---

## Example Workflow

1. **Setup:** Ensure your administrator has configured the `LLM_API_KEY` to enable the AI pipeline.
2. **Ingest Data:** Navigate to the API or Demo UI and create a new Corpus. Upload your source files.
3. **Select/Upload a Codebook:** Provide a hierarchical tree of themes (in JSON or CSV format, depending on your setup) that represents what you want to extract.
4. **Run Analysis:** Navigate to the "Trigger Analysis" page. You can optionally name your run. You may choose a specific Codebook and select the transcripts you wish to run against from the list. *Note: The system will warn you if you attempt to submit a run with the exact same Codebook and Transcripts as a previous run.*
5. **Review Results:** You can track the progress and status of your analysis runs in the "Previous Analysis Runs" table located beneath the trigger form. Explore the knowledge graph to discover insights, validate mappings, and track overarching themes across all documents.
