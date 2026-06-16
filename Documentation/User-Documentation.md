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

### 2. Codebook Interaction
A **Codebook** defines the themes you are looking for. It acts as a hierarchical structure of nodes/themes.
- **Select Codebooks:** Use the UI to explore different Codebook structures.
- **Thematic Mapping:** The AI pipeline uses Large Language Models (LLMs) to automatically map chunks of text from your documents to relevant themes in the Codebook.

### 3. Graph Exploration
The system builds a queryable thematic knowledge graph based on your analysis.
- **Theme Frequency Tracking:** Easily see which themes appear most frequently across your corpus.
- **Traceability:** Click on a mapped theme to view the exact text snippets (chunks) and source documents that generated the match.

---

## Example Workflow

1. **Setup:** Ensure your administrator has configured the `LLM_API_KEY` to enable the AI pipeline.
2. **Ingest Data:** Navigate to the API or Demo UI and create a new Corpus. Upload your source files.
3. **Select/Upload a Codebook:** Provide a hierarchical tree of themes (in JSON or CSV format, depending on your setup) that represents what you want to extract.
4. **Run Analysis:** Trigger the thematic mapping process. The LangChain pipeline will analyze each text chunk.
5. **Review Results:** Explore the knowledge graph to discover insights, validate mappings, and track overarching themes across all documents.
