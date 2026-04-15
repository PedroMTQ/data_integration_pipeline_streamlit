# vacuum_delta_tables

## Purpose

`VacuumDeltaTablesJob` removes old, unreferenced data files from all silver-layer Delta tables. Delta Lake retains old file versions for time-travel queries; vacuum reclaims that storage by deleting files older than a configurable retention period. This is a maintenance operation that complements `optimize_delta_tables`.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip vacuum-delta` |
| Airflow | `get_tasks()` returns one dict per Delta table; `process_task(task_dict)` vacuums a single table |

## How It Works

### Job layer (`VacuumDeltaTablesJob`)


1. **Discovery**: Same as `optimize_delta_tables` -- discovers all Delta tables under `silver/`.

2. **Processing (`process_data`)**:
   - Creates a `VacuumDeltaMetadata` with the `retention_hours` from `DELTA_VACUUM_RETENTION_HOURS` (default 168 hours / 7 days).
   - Calls `DeltaClient.vacuum_table()` with `dry_run=False`.
   - Records the number of files removed (`vacuum_files_removed`).
   - On failure, captures the error message.
   - Always marks metadata as complete.


## Metadata

| Field | Description |
|---|---|
| `data_source` | Data source identifier |
| `delta_table` | Full Delta table path being vacuumed |
| `retention_hours` | Files older than this are eligible for removal (default 168h) |
| `vacuum_files_removed` | Count of files actually deleted |
| `error_message` | Error string if vacuum failed |


## Design and Reasoning

- **Separate from optimize**: Optimize compacts files; vacuum removes old versions. They serve different purposes and have different risk profiles (vacuum is destructive -- it permanently removes time-travel capability for deleted versions).
- **Retention-based safety**: The default 168-hour retention window ensures that recent time-travel queries still work. Only files older than the retention period are removed.
- **`dry_run=False`**: The job performs actual deletion. In environments where a dry-run preview is needed, the parameter can be toggled, though currently the code always executes the vacuum.

## Variables

| Variable | Default | Description |
|---|---|---|
| `DELTA_VACUUM_RETENTION_HOURS` | `168` (7 days) | Files older than this retention window are eligible for deletion |

