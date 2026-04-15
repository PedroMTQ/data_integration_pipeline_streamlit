# optimize_delta_tables

## Purpose

`OptimizeDeltaTablesJob` compacts small files in all silver-layer Delta tables into larger, more efficient files and optionally applies Z-ordering for improved query performance (as of now we don't apply any z-ordering to columns, we just apply a simple optimize). This is a maintenance operation that should be run periodically after data has been written to Delta tables.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip optimize-delta` |
| Airflow | `get_tasks()` returns one dict per Delta table; `process_task(task_dict)` optimizes a single table |

## How It Works

### Job layer (`OptimizeDeltaTablesJob`)


1. **Discovery (`get_data_to_process`)** (inherited from `BaseDeltaMaintenanceJob`):
   - Uses `CloudStorageClient.get_delta_tables(prefix=SILVER_DATA_FOLDER)` to discover all Delta tables under `silver/`.
   - Yields each table path as a string.

2. **Processing (`process_data`)**:
   - Creates an `OptimizeDeltaMetadata` with settings from the environment: `target_size`, `max_concurrent_tasks`, `min_commit_interval_seconds`, `z_order_columns`.
   - Calls `DeltaClient.optimize_table()` with these parameters.
   - Captures the returned optimize metrics dict (files added/removed, bytes written, etc.).
   - On failure, captures the error message in metadata.
   - Always marks metadata as complete in a `finally` block.



## Metadata


| Field | Description |
|---|---|
| `data_source` | Data source identifier (resolved from table path via `ModelMapper`) |
| `delta_table` | Full Delta table path being optimized |
| `target_size` | Target file size in bytes (default 100 MB) |
| `max_concurrent_tasks` | Max parallel compaction tasks (default: CPU count) |
| `min_commit_interval_seconds` | Minimum seconds between commits during optimization (default 60s) |
| `z_order_columns` | Columns to Z-order by (default: none, configurable via env) |
| `optimize_metrics` | Dict of metrics returned by the Delta optimize operation |
| `error_message` | Error string if optimization failed |

## Variables

| Variable | Default | Description |
|---|---|---|
| `DELTA_OPTIMIZE_TARGET_SIZE` | `104857600` (100 MB) | Target file size in bytes for compaction; 100 MB is the standard Delta Lake recommendation |
| `DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS` | CPU count | Maximum parallel compaction tasks |
| `DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS` | `60` | Minimum seconds between commits during a long-running optimize |
| `DELTA_OPTIMIZE_Z_ORDER_COLUMNS` | *(empty — disabled)* | Comma-separated column names to Z-order by; left empty for simple compaction only |

