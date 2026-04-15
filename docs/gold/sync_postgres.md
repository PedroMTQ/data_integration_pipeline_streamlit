# sync_postgres

## Purpose

`SyncPostgresJob` streams records from the integrated silver Delta table into a PostgreSQL `integrated_records` table using upsert (INSERT ON CONFLICT) semantics. This is the first gold-layer step -- it makes the integrated data available for SQL queries and serves as the source of truth for the downstream Elasticsearch sync.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip sync-pg` |
| Airflow | `get_tasks()` returns at most one task dict; `process_task(task_dict)` runs the sync |

## How It Works

### Job layer (`SyncPostgresJob`)

1. **Resume resolution (`_resolve_last_synced_item`)**: Checks for the most recent `SyncIntegratedPostgresMetadata` via `get_latest()`. If the previous run did not complete (`sync_finished=False`), extracts the `last_synced_item` to enable resumption from where it left off.

2. **Discovery (`get_data_to_process`)**:
   - Verifies that `silver/integrated/records.delta` exists in S3.
   - Performs a staleness check (`_output_is_stale`): compares the Delta table's last commit timestamp against the Postgres table's `MAX(sync_ldts)`. If Postgres is up-to-date, the sync is skipped.
   - Yields a single task dict with `input_delta_table` and `output_postgres_table`.

3. **Processing (`process_data`)**:
   - Creates `SyncIntegratedPostgresMetadata` with the resolved resume state.
   - Instantiates `SyncIntegratedPostgresProcessor` and calls `processor.run()`.
   - Marks metadata as complete.

### Core layer (`SyncIntegratedPostgresProcessor`)

The processor in `gold/core/sync_integrated_postgres_processor.py` handles the actual data movement:

1. **Schema setup (`_ensure_schema`)**: Uses `SchemaManager` to load and execute SQL files from `gold/schemas/` (specifically `pg_integrated_gold.sql`). Checks whether tables already exist before executing, making the operation idempotent.

2. **Resume validation (`_validate_resume`)**: If resuming from a previous run, checks that the Delta table's current LDTS matches the stored LDTS. If the table has been updated since the last partial sync, the resume state is reset to avoid producing inconsistent data.

3. **Data streaming (`yield_data`)**: Reads the Delta table using `DeltaClient.read_fragments()`:
   - Iterates over Delta file fragments, skipping fragments at or before the resume point (`min_fragment_index`).
   - For each row, transforms the silver record into a gold record via `IntegratedMetaModel.from_silver_record()` and `.gold_record.model_dump()`.
   - Logs metrics and periodically persists the `last_synced_item` via `sync_timed_trigger` (default every 10 minutes).
   - After completing each fragment, updates the in-memory resume state.
   - On exception or `KeyboardInterrupt`, persists the current resume state before re-raising.

   Decorated with `@retry` for Delta read resilience (configurable retries, backoff, jitter).

4. **Postgres loading (`load_data`)**: Buffers yielded records and flushes in batches of `db_batch_size` via `_flush()`:
   - `_flush` calls `PostgresClient.upsert()` with `INSERT ON CONFLICT` semantics using the gold primary key (`hk`) as the conflict key and `hdiff` (hash diff) as the change-detection column.
   - The `WHERE ... IS DISTINCT FROM EXCLUDED` clause ensures that only actually-changed records are updated, minimizing write amplification.
   - Also decorated with `@retry` for Postgres resilience.

5. **Finalization**: Sets `sync_finished=True` in metadata.


## Metadata


| Field | Description |
|---|---|
| `input_delta_table` | Source: `silver/integrated/records.delta` |
| `output_postgres_table` | Target: `integrated_records` |
| `output_db_backend_config` | `PostgresBackendConfig` (host, port, user, database, batch size) |
| `n_input_records` | Count of records read from Delta |
| `n_output_records` | Count of records upserted to Postgres |
| `last_synced_item` | `LastSyncedItem(item=fragment_index, ldts=commit_timestamp)` for resume |
| `sync_finished` | Whether the sync completed successfully |



**Fragment indices are stable only within a single table version**: The resume mechanism tracks the last-processed fragment index and the Delta table's LDTS. If the table is rewritten (new LDTS), the resume state resets and a full re-sync occurs. But if the table is compacted (via `optimize_delta`) *without* changing the LDTS, fragment indices may shift and the resume could skip or re-process data. In practice, optimize changes the LDTS, but this coupling is implicit.

## Variables

| Variable | Default | Description |
|---|---|---|
| `DELTA_CLIENT_BATCH_SIZE` | `100000` | Batch size when reading Delta table fragments |
| `DELTA_CLIENT_WRITE_RETRIES` | `5` | Retries on Delta read failures |
| `DELTA_CLIENT_WRITE_RETRY_DELAY` | `10` | Base delay (seconds) between Delta read retries |
| `POSTGRES_CLIENT_BATCH_SIZE` | `10000` | Rows per `INSERT ON CONFLICT` batch |
| `POSTGRES_MAX_RETRIES` | `10` | Retries on Postgres upsert failures |
| `POSTGRES_RETRY_DELAY` | `1.0` | Base delay (seconds) between Postgres retries |
| `POSTGRES_CONNECTION_TIMEOUT` | `10` | Seconds before a Postgres connection attempt times out |
