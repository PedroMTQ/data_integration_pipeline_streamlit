# sync_elastic_search

## Purpose

`SyncOrganizationsElasticsearchJob` streams records from the PostgreSQL `integrated_records` table into an Elasticsearch index, enabling full-text search, keyword filtering, and semantic (vector) search over the integrated dataset. This is the final data movement step in the pipeline -- after this job completes, records are queryable via the `QueryClient`.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip sync-es` |
| Airflow | This job does not expose `get_tasks` / `process_task` -- it runs as a single unit because the entire Postgres table feeds one ES index. |


## How It Works

### Job layer (`SyncOrganizationsElasticsearchJob`)

1. **Resume resolution (`_resolve_last_synced_item`)**: Checks the most recent `SyncElasticSearchMetadata` via `get_latest()`. If the previous run did not complete, extracts `last_synced_item` (the last successfully-indexed primary key) to enable resumption.

2. **Run**: Creates `SyncElasticSearchMetadata` with both input (`PostgresBackendConfig`) and output (`ElasticsearchBackendConfig`) connection details, then delegates to `SyncElasticSearchProcessor.run()`.

### Core layer (`SyncElasticSearchProcessor`)

The processor in `gold/core/sync_elastic_search_processor.py` handles the Postgres-to-ES data movement:

1. **Connection validation**: Pings both the Postgres and Elasticsearch backends on construction to fail fast if either is unreachable.

2. **Index initialization (`run`)**: Calls `ElasticsearchClient.init_index()` which creates the ES index from the template in `es_integrated_gold.py` if it does not exist, and sets up the index alias.

3. **Data streaming (`yield_data`)**:
   - Builds a SQL `SELECT * FROM integrated_records` query, optionally with a `WHERE hk > last_synced_pk ORDER BY hk` clause for resume.
   - Uses a **server-side cursor** (`cursor.stream()`) to avoid materializing the full result set in memory.
   - For each row, converts the Postgres row dict into an `ElasticsearchSchemaRecord` via `from_postgres_row()`, which reshapes the flat relational row into the ES document format (including embedding vectors).
   - Yields the document dict.
   - Periodically triggers metric logging and metadata persistence.

   Decorated with `@retry` for Postgres read resilience.

4. **ES loading (`load_data`)**:
   - Passes the `yield_data()` stream to `ElasticsearchClient.load_stream()`, which uses the ES `streaming_bulk` helper for efficient batch indexing.
   - For each successfully indexed document, extracts the record ID from the bulk response and updates `metadata.last_synced_item`.
   - On failure, raises a `RuntimeError` with the bulk item error details.
   - On any exception or `KeyboardInterrupt`, persists the current resume state.

   Decorated with `@retry` for ES write resilience.

5. **Finalization**: Sets `sync_finished=True`.


## Metadata


| Field | Description |
|---|---|
| `input_postgres_table` | Source: `integrated_records` |
| `output_es_index` | Target: `integrated_records` (alias) |
| `input_db_backend_config` | `PostgresBackendConfig` for reading |
| `output_db_backend_config` | `ElasticsearchBackendConfig` (URLs, index name, alias, shard count, batch size, retry settings) |
| `n_input_records` | Count of records read |
| `n_output_records` | Count of records indexed |
| `last_synced_item` | `LastSyncedItem(item=last_hk)` for resume |
| `sync_finished` | Whether the sync completed |

## Variables

| Variable | Default | Description |
|---|---|---|
| `EMBEDDING_DIMENSIONS` | `256` | Dense vector `dims` in the ES index mapping template |
| `ELASTICSEARCH_INDEX_NAME` | `integrated_records_v1` | Physical ES index name |
| `ELASTICSEARCH_INDEX_ALIAS` | `integrated_records` | ES index alias used by the `QueryClient` |
| `ELASTICSEARCH_NUMBER_OF_SHARDS` | `1` | Shard count in the index template (1 since we only have 1 ES node) |
| `ES_CLIENT_BATCH_SIZE` | `100` | Documents per `streaming_bulk` chunk |
| `ES_CLIENT_RAISE_ERRORS` | `0` (false) | Whether the bulk helper raises on per-document errors |
| `ES_MAX_RETRIES` | `10` | Retries on ES bulk write failures |
| `ES_RETRY_BACKOFF_SECONDS` | `1.0` | Initial backoff (seconds) between ES retries |
| `ES_RETRY_MAX_BACKOFF_SECONDS` | `30.0` | Maximum backoff cap for ES retries |
| `POSTGRES_CLIENT_BATCH_SIZE` | `10000` | Fetch size for the server-side cursor reading from Postgres |
