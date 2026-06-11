# Backend Software Architecture and Data Model Documentation

Source: generated from the `classes_BackendApp.dot` pyreverse class graph.

This document presents a non-decorative architectural abstraction of the backend. It separates runtime responsibilities from persistence structures and avoids implementation-specific visual clutter.

## 1. Architectural Overview

The backend is organized as a layered application. The observable structure indicates the following principal layers:

1. **Interface layer**: request/response schemas, pagination envelopes, middleware, and validation contracts.
2. **Application service layer**: procedural business operations for ingestion, demographic processing, theme graph computation, theme reading, and frequency aggregation.
3. **Domain/data model layer**: SQLAlchemy-backed persistent entities representing corpora, documents, demographic files, codebooks, themes, analyses, and theme occurrences.
4. **Infrastructure and cross-cutting layer**: configuration, logging, and domain-specific exception classes.

![Layer diagram](software_architecture_mid_release.png)

```mermaid
flowchart TB
    subgraph Interface_Layer[Interface Layer]
        MW[RequestIdMiddleware and RequestLoggingMiddleware]
        SC[Pydantic Schemas: requests, responses, pagination, envelopes]
    end

    subgraph Application_Service_Layer[Application Service Layer]
        IS[IngestionService]
        DS[DemographicService]
        TGS[ThemeGraphService]
        TRS[ThemeReadService]
        TFS[ThemeFrequencyService]
        TC[Text Chunking]
        LLM[LLM Analysis Contracts]
    end

    subgraph Domain_Data_Model_Layer[Domain and Data Model Layer]
        IM[Ingestion Models: Corpus, CorpusDocument]
        DM[Demographic Models: DemographicFiles, DemographicRow]
        CM[Codebook and Theme Models: Codebook, CodebookGenerationJob, Theme, Code, relationships]
        AM[Analysis Models: DocumentAnalysis, ThemeOccurrence]
        BM[Base and Timestamp Mixins]
    end

    subgraph Infrastructure_Layer[Infrastructure and Cross-Cutting Layer]
        CFG[Settings]
        EXC[Application Exceptions]
        LOG[Logging Configuration]
        DB[(Relational Database)]
    end

    MW --> SC
    SC --> IS
    SC --> DS
    SC --> TGS
    SC --> TRS
    SC --> TFS

    IS --> TC
    IS --> IM
    DS --> DM
    DS --> IM
    TGS --> CM
    TRS --> CM
    TFS --> AM
    TFS --> CM
    LLM --> AM

    IM --> DB
    DM --> DB
    CM --> DB
    AM --> DB
    BM --> IM
    BM --> DM
    BM --> CM
    BM --> AM

    CFG --> IS
    CFG --> DS
    CFG --> LLM
    EXC --> IS
    EXC --> DS
    EXC --> TGS
    LOG --> MW
```

## 2. Main Service Responsibilities

| Service or component | Scientific role | Primary data dependency |
|---|---|---|
| `IngestionService` | Creates corpora, ingests documents (`.txt`, `.docx`, `.pdf`, `.jsonl`), and lists corpus artifacts. | `Corpus`, `CorpusDocument` |
| `DemographicService` | Imports demographic CSV-like data, confirms temporary uploads, lists demographic records, and links demographic rows to transcripts. | `DemographicFiles`, `DemographicRow`, `CorpusDocument` |
| `ThemeGraphService` | Constructs and validates a directed acyclic graph of themes within a codebook. | `Theme`, `ThemeHierarchyRelationship`, `Codebook` |
| `ThemeReadService` | Provides a readable tree representation of the theme hierarchy. | `Theme`, `ThemeHierarchyRelationship` |
| `ThemeFrequencyService` | Computes aggregate theme occurrence statistics across analyses. | `ThemeOccurrence`, `Theme`, `DocumentAnalysis` |
| `CodebookGenerationService` | Splits document content into passages at runtime, invokes the LLM pipeline per passage, and consolidates generated themes and codes into a persisted codebook. | `CodebookGenerationJob`, `Codebook`, `Theme`, `Code` |
| LLM analysis contracts | Represent structured model output before persistence into analysis tables. | `InterviewAnalysisResult`, `ThemePresence`, `DocumentAnalysis`, `ThemeOccurrence` |

## 3. Persistent Data Model

The following model is a conceptual ERM derived from the class graph and the foreign-key-like attributes visible in the model classes.

![Data Model Diagram](data_model_mid_release.png)

```mermaid
erDiagram
    CORPUS ||--o{ CORPUS_DOCUMENT : contains
    CORPUS ||--o{ CODEBOOK : has

    CORPUS ||--o{ DEMOGRAPHIC_FILES : has
    CORPUS ||--o{ DEMOGRAPHIC_ROW : has
    DEMOGRAPHIC_FILES ||--o{ DEMOGRAPHIC_ROW : contains
    DEMOGRAPHIC_ROW ||--o{ CORPUS_DOCUMENT : may_link_to

    CORPUS_DOCUMENT ||--o{ DOCUMENT_ANALYSIS : has
    CODEBOOK ||--o{ DOCUMENT_ANALYSIS : constrains

    CODEBOOK ||--o{ CODEBOOK_THEME_RELATIONSHIP : has
    THEME ||--o{ CODEBOOK_THEME_RELATIONSHIP : participates_in

    CODEBOOK ||--o{ THEME_HIERARCHY_RELATIONSHIP : scopes
    THEME ||--o{ THEME_HIERARCHY_RELATIONSHIP : parent_role
    THEME ||--o{ THEME_HIERARCHY_RELATIONSHIP : child_role

    CODEBOOK ||--o{ CODEBOOK_GENERATION_JOB : produced_by

    DOCUMENT_ANALYSIS ||--o{ THEME_OCCURRENCE : produces
    THEME ||--o{ THEME_OCCURRENCE : observed_as

    CORPUS {
        UUID id PK
        UUID project_id
        string name
        datetime created_at
        datetime updated_at
    }

    CORPUS_DOCUMENT {
        UUID id PK
        UUID corpus_id FK
        UUID demographic_row_id FK
        string title
        string filename_optional
        text content
        datetime created_at
        datetime updated_at
    }

    DEMOGRAPHIC_FILES {
        UUID id PK
        UUID corpus_id FK
        string name
        list original_columns
        datetime created_at
        datetime updated_at
    }

    DEMOGRAPHIC_ROW {
        UUID id PK
        UUID corpus_id FK
        UUID demographic_file_id FK
        string interviewee_id
        int row_number
        dict data
    }

    CODEBOOK {
        UUID id PK
        UUID corpus_id FK
        string name
        string description_optional
        int version
        string created_by
        datetime created_at
        datetime updated_at
    }

    CODEBOOK_GENERATION_JOB {
        UUID id PK
        string status
        string codebook_name
        UUID corpus_id
        text transcript_document_ids_json
        bool cancel_requested
        UUID codebook_id_optional
        int passages_total
        int passages_done
        string error_message_optional
        datetime started_at_optional
        datetime finished_at_optional
        datetime created_at
        datetime updated_at
    }

    THEME {
        UUID id PK
        UUID codebook_id FK
        string label
        string description_optional
        bool is_active
        datetime created_at
        datetime updated_at
    }

    CODEBOOK_THEME_RELATIONSHIP {
        UUID id PK
        UUID codebook_id FK
        UUID theme_id FK
        bool is_active
        datetime created_at
        datetime updated_at
    }

    THEME_HIERARCHY_RELATIONSHIP {
        UUID id PK
        UUID codebook_id FK
        UUID parent_theme_id FK
        UUID child_theme_id FK
        bool is_active
        datetime created_at
        datetime updated_at
    }

    DOCUMENT_ANALYSIS {
        UUID id PK
        UUID document_id FK
        UUID codebook_id FK
        string summary_optional
        string researcher_notes_optional
        datetime created_at
        datetime updated_at
    }

    THEME_OCCURRENCE {
        UUID id PK
        UUID analysis_id FK
        UUID theme_id FK
        bool is_present
        float confidence
        string quote_optional
        datetime created_at
        datetime updated_at
    }
```

## 4. Entity Catalogue

### 4.1 Corpus and document ingestion

| Entity | Description | Key attributes |
|---|---|---|
| `Corpus` | Logical collection of source documents within a project. | `id`, `project_id`, `name` |
| `CorpusDocument` | A single ingested document or transcript. Stores both metadata and the full document text. It may be linked to one demographic row. | `id`, `corpus_id`, `demographic_row_id`, `title`, `filename`, `content` |

### 4.2 Demographic data

| Entity | Description | Key attributes |
|---|---|---|
| `DemographicFiles` | Imported demographic file metadata and original column structure. | `id`, `corpus_id`, `name`, `original_columns` |
| `DemographicRow` | One normalized row of imported demographic data. | `id`, `corpus_id`, `demographic_file_id`, `interviewee_id`, `row_number`, `data` |

### 4.3 Codebook and theme graph

| Entity | Description | Key attributes |
|---|---|---|
| `Codebook` | Versioned coding framework associated with a corpus. | `id`, `corpus_id`, `name`, `description`, `version`, `created_by` |
| `CodebookGenerationJob` | Background job that generates a codebook from corpus transcripts via the LLM pipeline. | `id`, `status`, `codebook_name`, `corpus_id`, `cancel_requested`, `passages_total`, `passages_done`, `codebook_id`, `error_message` |
| `Theme` | A node in the codebook hierarchy (theme, subtheme, or code label). | `id`, `codebook_id`, `label`, `description`, `is_active` |
| `CodebookThemeRelationship` | Association table mapping themes into codebooks. | `id`, `codebook_id`, `theme_id`, `is_active` |
| `ThemeHierarchyRelationship` | Directed edge between parent and child themes within a codebook. | `id`, `codebook_id`, `parent_theme_id`, `child_theme_id`, `is_active` |

### 4.4 Analysis

| Entity | Description | Key attributes |
|---|---|---|
| `DocumentAnalysis` | Analysis result for a document under a specific codebook. | `id`, `document_id`, `codebook_id`, `summary`, `researcher_notes` |
| `ThemeOccurrence` | Observation of a theme within a document analysis, including evidence and confidence. | `id`, `analysis_id`, `theme_id`, `is_present`, `confidence`, `quote` |

