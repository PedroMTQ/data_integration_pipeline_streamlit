# streamlit dashboard

## Purpose

The Streamlit dashboard is the interactive UI for exploring pipeline health, data quality, and retrieval behavior without running SQL queries manually.

Entrypoint:

- CLI: `dip streamlit`

Default URL: `http://localhost:8501`

## Navigation

The app has three pages configured via `st.navigation(...)`:

1. **Pipeline Overview**
2. **Data Quality**
3. **Search & Retrieval**

`Pipeline Overview` is the default landing page.

## Page guide

### 1) Pipeline Overview

This page provides:

- Architecture summary table (Raw -> Bronze -> Silver -> Gold).
- Record counts for:
  - Silver Delta tables (`dataset_1`, `dataset_2`, `dataset_3`, `integrated`)
  - Gold serving layers (PostgreSQL and Elasticsearch)

Counts are cached with `st.cache_data` using `STREAMLIT_CACHE_TTL`.

### 2) Data Quality

This page has four sections:

1. **Integration Report** (`integrated_records_report` Postgres view)
   - Core volume and uniqueness KPIs (`total_records`, distinct IDs/URLs, duplicates)
   - Revenue bound consistency
   - Coverage tabs:
     - Dataset 1 seed coverage (`d1_has_*`)
     - Dataset 2 enrichment coverage (`d2_has_*`)
     - Dataset 3 embedding coverage (`d3_has_*`)
2. **Audit Expectation Rules**
   - Static mirror of key Great Expectations rules used for silver auditing.
3. **Silver Delta Table Schemas**
   - Schema browser for dataset 1/2/3 + integrated Delta tables.
4. **Gold Field Completeness**
   - Non-null percentages for selected columns in Postgres `integrated_records`.

If the integration report is missing or stale, refresh it with:

```bash
dip pg-report
```

### 3) Search & Retrieval

Single unified search page for BM25, semantic, and hybrid retrieval.

Inputs:

- `BM25 text search` (`query_text`)
- `Semantic search` (`semantic_query`)
- Optional structured filters:
  - Country / ISO2
  - Employee range
  - Founded year range
  - Revenue range

Semantic options:

- `num_candidates` for HNSW candidate pool size.
- Embedding mode:
  - `Global` (`embedding_global_vector`)
  - `Per-field` (choose specific embedding fields)

BM25 options:

- `query_text` always uses a BM25 `must` query.
- You can choose BM25 text fields via **BM25 search options**.
- The dashboard defaults to all `BM25_TEXT_FIELDS` selected.
- If no BM25 fields are selected, the BM25 text clause is skipped.
- If `query_text` is provided while BM25 fields are empty and no semantic query is set, the UI prompts you to select BM25 fields (or use semantic query) instead of running a broad search.

Examples:

- "Example queries" buttons populate current widget state and immediately rerun.
- This lets you start from a known scenario and then tweak filters manually.

## Caching behavior

The dashboard uses two Streamlit cache layers:

- `st.cache_resource` for shared clients (Postgres/Elasticsearch/Delta)
- `st.cache_data` for expensive reads (counts, report row, schema fetches)

Cache TTL is controlled by `STREAMLIT_CACHE_TTL` in `settings.py`.

When debugging stale UI data:

1. Re-run source jobs (`dip sync-pg`, `dip sync-es`, `dip pg-report`)
2. Wait for cache TTL to expire, or restart Streamlit

**Note that the embeddings model is loaded lazily, and so the firt you run a semantic search, the results will take some time to load! Posterior searches will be immediate**

## Operational checklist

Before using the dashboard end-to-end, ensure:

1. Integrated records is synced to Postgres: `dip sync-pg`
2. Search index is synced to ES: `dip sync-es`
3. Report view exists: `dip pg-report`
4. Streamlit is running: `dip streamlit`

