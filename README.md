# Research Paper Airflow ETL Pipeline for RAG Research Assistant

## Overview

This project implements an **ETL (Extract, Transform, Load) pipeline** using **Apache Airflow** to build a data foundation for a **RAG (Retrieval-Augmented Generation) research assistant**. The pipeline fetches research papers from arXiv, processes them, and stores structured data in PostgreSQL for downstream AI applications.

## Architecture

### Asset-Based DAG Orchestration

The pipeline uses **Airflow Assets** (formerly Datasets) to create a reactive, event-driven ETL workflow:

```
┌─────────────────┐
│   Orchestrator  │ (Manual/Scheduled Trigger)
│      DAG        │
└────────┬────────┘
         │
         ├──► Extract DAG ──► [arxiv_asset] ──► Transform DAG ──► [transformed_asset] ──► Load DAG
         │         │                                    │                                      │
         │         └─► data_extract.xml                └─► data_transform.json                └─► PostgreSQL
         │
         └──► Triggers each stage sequentially
```

### Key Components

#### 1. **Extract DAG** (`extract.py`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Fetches research papers from arXiv API
- **Output Asset**: `/opt/airflow/logs/data/data_extract.xml`
- **Features**:
  - Incremental extraction using counter file
  - Fetches 50 papers per run (configurable)
  - Rate limiting (3-second delay between requests)
  - Stores raw XML responses

#### 2. **Transform DAG** (`transform.py`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Parses XML and extracts PDF content
- **Input**: Consumes `arxiv_asset` (XML file)
- **Output Asset**: `/opt/airflow/logs/data/data_transform.json`
- **Features**:
  - XML parsing with namespace handling
  - PDF download and text extraction (first 3 pages)
  - Content cleaning and paragraph extraction
  - Structured JSON output with metadata

#### 3. **Load DAG** (`load.py`)
- **Schedule**: Asset-triggered by `transformed_asset`
- **Purpose**: Loads processed data into PostgreSQL
- **Features**:
  - Automatic table creation
  - JSONB storage for arrays and nested data
  - Stores paper metadata, summaries, and PDF content

#### 4. **Orchestrator DAG** (`orchestrator.py`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Coordinates the entire ETL pipeline
- **Features**:
  - Sequential execution: Extract → Transform → Load
  - Waits for each stage to complete before proceeding
  - Centralized pipeline management

## Asset-Driven Workflow

### What are Airflow Assets?

Assets are Airflow's mechanism for **data-aware scheduling**. Instead of time-based triggers, DAGs react to data availability:

1. **Extract DAG** produces `arxiv_asset` (XML file)
2. **Transform DAG** automatically triggers when `arxiv_asset` is updated
3. **Load DAG** automatically triggers when `transformed_asset` is updated

### Benefits

- **Decoupled DAGs**: Each stage is independent and reusable
- **Event-Driven**: Downstream tasks only run when data is ready
- **Fault Tolerance**: Failed stages don't block unrelated workflows
- **Scalability**: Easy to add new consumers of the same assets

## Data Flow

### 1. Extraction Phase
```python
arXiv API → Raw XML → data_extract.xml
```
- Queries: `search_query=all`
- Pagination: 10 results per page
- Counter tracking: Maintains position across runs

### 2. Transformation Phase
```python
XML → Parsed Metadata + PDF Content → data_transform.json
```
**Extracted Fields**:
- `id`: arXiv paper ID
- `title`: Paper title
- `authors`: List of author names
- `summary`: Abstract
- `categories`: Subject classifications
- `pdf_link`: Direct PDF URL
- `pdf_content`: First 10 cleaned paragraphs from PDF

### 3. Loading Phase
```python
JSON → PostgreSQL (research_papers table)
```
**Schema**:
```sql
CREATE TABLE research_papers (
    id SERIAL PRIMARY KEY,
    search_query VARCHAR(255),
    paper_id VARCHAR(255),
    title TEXT,
    updated TIMESTAMP,
    published TIMESTAMP,
    summary TEXT,
    authors JSONB,
    categories JSONB,
    pdf_link TEXT,
    pdf_content JSONB
);
```

## RAG Integration

This pipeline prepares data for RAG applications by:

1. **Structured Storage**: PostgreSQL enables efficient querying
2. **Text Extraction**: PDF content ready for embedding generation
3. **Metadata Preservation**: Authors, categories, dates for filtering
4. **Incremental Updates**: Daily additions without reprocessing

### Typical RAG Workflow
```
User Query → Vector Search (pdf_content embeddings) → Retrieve Relevant Papers → LLM Context → Response
```

## Infrastructure

### Docker Compose Stack

- **Airflow API Server**: Web UI (port 8080)
- **Scheduler**: DAG execution orchestration
- **Worker**: Task execution (Celery)
- **Triggerer**: Async task handling
- **PostgreSQL**: Metadata DB + data warehouse
- **Redis**: Celery message broker
- **pgAdmin**: Database management (port 5050)

### Configuration

**Environment Variables** (`.env`):
```bash
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=pdfplumber
```

**Dependencies** (`pyproject.toml`):
- `apache-airflow>=3.1.7`
- `pdfplumber>=0.11.9` (PDF text extraction)
- `requests>=2.32.5` (HTTP requests)
- `psycopg2` (PostgreSQL adapter)

## Setup & Deployment

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM
- 10GB+ disk space

### Quick Start

1. **Clone and Navigate**:
   ```bash
   cd Research_Paper_Airflow_ETL
   ```

2. **Start Services**:
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow UI**:
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

4. **Trigger Pipeline**:
   - Enable `arxiv_etl_orchestrator` DAG
   - Click "Trigger DAG" or wait for daily schedule

### Monitoring

- **Airflow UI**: DAG runs, task logs, asset lineage
- **pgAdmin**: http://localhost:5050 (admin@admin.com / admin)
- **Logs**: `./logs/dag_id=<dag_name>/`

## Project Structure

```
Research_Paper_Airflow_ETL/
├── dags/
│   ├── extract.py          # arXiv API extraction
│   ├── transform.py        # XML parsing + PDF processing
│   ├── load.py             # PostgreSQL loading
│   └── orchestrator.py     # Pipeline coordination
├── logs/
│   ├── data/               # Asset storage
│   │   ├── data_extract.xml
│   │   ├── data_transform.json
│   │   └── count.txt       # Extraction counter
│   └── dag_id=*/           # Execution logs
├── config/
│   └── airflow.cfg         # Airflow configuration
├── plugins/                # Custom operators (empty)
├── docker-compose.yaml     # Infrastructure definition
├── .env                    # Environment variables
└── pyproject.toml          # Python dependencies
```

## Key Features

### 1. Incremental Processing
- Counter file tracks last processed position
- Prevents duplicate downloads
- Resumes from last checkpoint

### 2. Error Handling
- PDF extraction failures logged but don't block pipeline
- Database transactions ensure data consistency
- Airflow retries on transient failures

### 3. Scalability
- Celery workers enable parallel task execution
- Asset-based triggering prevents unnecessary runs
- Modular DAGs allow independent scaling

### 4. Data Quality
- XML namespace handling for robust parsing
- PDF content cleaning (removes headers, normalizes whitespace)
- JSONB storage preserves complex structures

## Future Enhancements

1. **Vector Embeddings**: Add embedding generation task
2. **Vector Database**: Integrate Pinecone/Weaviate for similarity search
3. **Query Interface**: Build FastAPI endpoint for RAG queries
4. **Advanced Filtering**: Add category/date-based extraction
5. **Monitoring**: Add data quality checks and alerting

## Troubleshooting

### Common Issues

**DAGs not appearing**:
- Check `./dags/` folder is mounted correctly
- Verify no Python syntax errors: `docker-compose exec airflow-scheduler airflow dags list`

**Asset not triggering downstream DAGs**:
- Ensure asset URIs match exactly between producer and consumer
- Check asset lineage in Airflow UI: Browse → Assets

**PDF extraction failures**:
- Network timeouts: Increase timeout in `transform.py`
- Memory issues: Reduce concurrent PDF processing

**Database connection errors**:
- Verify PostgreSQL is healthy: `docker-compose ps`
- Check credentials in `load.py` match `docker-compose.yaml`

## License

Apache License 2.0 (inherited from Apache Airflow)

## Contributing

1. Fork the repository
2. Create feature branch
3. Test changes locally with `docker-compose`
4. Submit pull request

---

**Built with**: Apache Airflow 3.1.7 | PostgreSQL 16 | Python 3.12+
