# Data Integration Pipeline

An end-to-end data integration pipeline that ingests company data from three heterogeneous sources, validates and aligns it through a medallion architecture (Bronze → Silver → Gold), and serves the unified dataset through PostgreSQL (relational queries) and Elasticsearch (full-text and semantic search). Built for production scale, designed to handle hundreds of millions of records with incremental processing, idempotent writes, and resume-on-failure.

## Documentation

Full documentation is available **[here](https://pedromtq.github.io/data_integration_pipeline_streamlit/)** and covers:

- **[Overview](https://pedromtq.github.io/data_integration_pipeline_streamlit/)** -- architecture, data model, search & retrieval, assumptions
- **Bronze layer** -- [download](https://pedromtq.github.io/data_integration_pipeline_streamlit/bronze/download_bronze/), [upload](https://pedromtq.github.io/data_integration_pipeline_streamlit/bronze/upload_bronze/), [chunk](https://pedromtq.github.io/data_integration_pipeline_streamlit/bronze/chunk_bronze/), [process](https://pedromtq.github.io/data_integration_pipeline_streamlit/bronze/process_bronze/), [load](https://pedromtq.github.io/data_integration_pipeline_streamlit/bronze/load_bronze/), [errors](https://pedromtq.github.io/data_integration_pipeline_streamlit/bronze/list_bronze_errors/)
- **Silver layer** -- [integrate](https://pedromtq.github.io/data_integration_pipeline_streamlit/silver/integrate_silver/), [optimize](https://pedromtq.github.io/data_integration_pipeline_streamlit/silver/optimize_delta_tables/), [vacuum](https://pedromtq.github.io/data_integration_pipeline_streamlit/silver/vacuum_delta_tables/), [audit](https://pedromtq.github.io/data_integration_pipeline_streamlit/silver/audit_silver/)
- **Gold layer** -- [sync Postgres](https://pedromtq.github.io/data_integration_pipeline_streamlit/gold/sync_postgres/), [sync Elasticsearch](https://pedromtq.github.io/data_integration_pipeline_streamlit/gold/sync_elastic_search/), [PG report](https://pedromtq.github.io/data_integration_pipeline_streamlit/gold/create_pg_report/)


## Tech Stack

- **Language:** Python 3.13
- **Data lake:** Delta Lake (`deltalake` for Python-native reads/writes, `delta-spark` for PySpark integration)
- **Distributed compute:** PySpark (multi-source join at silver layer)
- **Serving database:** PostgreSQL 16
- **Search engine:** Elasticsearch 8.17 (full-text BM25, dense vector KNN, keyword filters)
- **Data validation:** Pydantic v2 (row-level schema enforcement), Great Expectations (statistical audits)
- **Data processing:** PyArrow, Polars (sampling and hashing)
- **Orchestration:** Apache Airflow 3.2 (CeleryExecutor, Docker task isolation)
- **Object storage:** MinIO (S3-compatible, local development)
- **Containerization:** Docker Compose (multi-service local environment)
- **Package management:** `uv`
- **Documentation:** `mkdocs`
- **Dashboard:** `Streamlit`

## Setup and Execution

### Prerequisites

- Docker and Docker Compose
- [uv](https://docs.astral.sh/uv/) (`wget -qO- https://astral.sh/uv/install.sh | sh`)

### Local Environment

```bash
# Install Python virtual environment
make install

# Activate the environment (sources dev.env, activates venv)
make activate
```

### Infrastructure

```bash
# Build the pipeline Docker image and Airflow image
# if you are plannning to use Airflow
make docker-build

# Start all services (MinIO, PostgreSQL, Elasticsearch, Airflow)
make docker-up
# or  if you don't want to use Airflow
make docker-infra-up

# Stop all services
make docker-down
```

**Note that when PG,Airflow, and ES are initialized I create a sentinel file to avoid launching the init containers. This speeds up the docker up/down; but note that you may have to remove the tmp/compose_init files if you restart the services and change configurations without first running `make docker-reset`**

### Service UIs

| Service | URL | User | Password |
|---------|-----|------|----------|
| Airflow | http://localhost:8080 | `airflow` | `airflow` |
| Kibana (ES) | http://localhost:5601 | `elastic` | `elastic123` |
| MinIO Console | http://localhost:9001 | `user123` | `password123` |



### Running the Pipeline

**Full pipeline (local demo):**

```bash
dip pipeline
```

**Individual stages:**

```bash
dip download-bronze    # Download test data from public bucket to local filesystem
dip upload-bronze      # Upload test data to S3
dip chunk-bronze       # Chunk raw files into Parquet and archive originals
dip process-bronze     # Validate chunks with Pydantic, write processed Parquet + errors
dip load-bronze        # Write validated data to per-source Silver Delta tables
dip audit-silver       # Run Great Expectations audits on Silver Delta tables
dip integrate-silver   # PySpark join into integrated Delta
dip sync-pg            # Stream integrated Delta to PostgreSQL
dip sync-es            # Index PostgreSQL records into Elasticsearch
```

**Maintenance:**

```bash
dip optimize-delta     # Compact Delta table files (Z-order if configured)
dip vacuum-delta       # Remove old Delta file versions beyond retention period
```

**Reporting and UIs:**

```bash
dip pg-report          # Create or refresh the summary materialized view in Postgres
dip audit-docs         # Serve Great Expectations HTML audit reports (default port 8088)
dip streamlit          # Launch Streamlit data explorer UI (default port 8501)
```

