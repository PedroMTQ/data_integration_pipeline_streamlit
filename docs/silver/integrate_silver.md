# integrate_silver

## Purpose

`IntegrateSilverJob` joins the three per-source silver Delta tables (`dataset_1`, `dataset_2`, `dataset_3`) into a single unified `silver/integrated/records.delta` table using Apache Spark. This is the core data integration step -- it combines records from different sources into one canonical schema, resolving column mappings and cross-source joins based on the integrated silver schema definition.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip integrate-silver` |
| Airflow | `get_tasks()` returns at most one task dict; `process_task(task_dict)` runs the integration |

## How It Works

### Job layer (`IntegrateSilverJob`)

1. **Discovery (`get_data_to_process`)**:
   - Lists all Delta tables under the `silver/` prefix.
   - Checks that all three input tables exist: `silver/dataset_1/records.delta`, `silver/dataset_2/records.delta`, `silver/dataset_3/records.delta`.
   - Performs a **staleness check** (`_output_is_stale`): compares the last commit timestamp of each input table against the output table. If all inputs are older than the output, integration is skipped.
   - Resolves the `seed_data_source` from the integrated silver schema (the primary/seed dataset that drives the join).
   - Yields a single task dict with `seed_data_source`, `input_delta_tables`, and `output_delta_table`.

2. **Processing (`process_data`)**:
   - Creates an `IntegrateSilverMetadata` object.
   - Instantiates `IntegrateSilverProcessor` with the metadata and calls `processor.run()`.
   - Marks the metadata as complete.

### Core layer (`IntegrateSilverProcessor`)

The `IntegrateSilverProcessor` in `silver/core/integrate_silver_processor.py` orchestrates the Spark-based integration:

1. **Column mapping derivation (`_derive_column_mappings`)**: Inspects the integrated silver Pydantic schema. For each field:
   - If it has an `AliasPath(source, column)` validation alias, maps it as `(source, source_col, output_col)`.
   - Otherwise, maps it from the `seed_data_source` using the field name directly.
   - This produces a list of `(data_source, source_column, output_column)` tuples that drive the join and select.

2. **Read sources (`_read_sources`)**: Reads each input Delta table into a Spark DataFrame via `SparkClient.read()`. Logs row counts.

3. **Join (`_join`)**:
   - Aliases each DataFrame by its data source name.
   - Performs left joins from the seed dataset to each other dataset on the `join_key`.
   - Selects output columns using the derived column mappings, applying aliases where source and output names differ.
   - Records `n_input_records` (count of seed dataset rows).

4. **Validate (`_validate`)**: Asserts that the joined DataFrame's schema matches the `IntegratedMetaModel._pa_silver_schema` using `SparkClient.assert_schema_matches`. This is a contract check that catches schema drift.

5. **Write (`_write`)**: Writes the joined DataFrame to the output Delta table via `SparkClient.write()` with the configured `primary_key` and `partition_key`. Records `n_output_records`.

6. **Error handling**: If any step fails, metrics are logged as failure and the Spark session is stopped in a `finally` block.


## Metadata


| Field | Description |
|---|---|
| `seed_data_source` | The primary dataset that drives the left join |
| `input_delta_tables` | List of the three input Delta table paths |
| `output_delta_table` | Target: `silver/integrated/records.delta` |
| `n_input_records` | Row count of the seed dataset |
| `n_output_records` | Row count of the output table after write |
| `sync_finished` | Whether the integration completed successfully |

## Variables

| Variable | Default | Description |
|---|---|---|
| `EMBEDDING_DIMENSIONS` | `256` | Vector dimensionality used in the integrated silver schema |
| `EMBEDDING_LDTS_COLUMN` | `embedding_ldts` | Embedding timestamp field name in the integrated schema |
| `SPARK_APP_NAME` | `dip-spark` | Spark application name |
| `SPARK_MASTER` | `local[*]` | Spark master URL (`local[*]` for local, a cluster URL for distributed) |
| `SPARK_HOST` | `127.0.0.1` | Spark driver bind address |
| `SPARK_DRIVER_MEMORY` | `4g` | Memory allocated to the Spark driver |
| `SPARK_EXECUTOR_MEMORY` | `4g` | Memory allocated to each Spark executor |
| `SPARK_CLIENT_WRITE_RETRIES` | `5` | Number of retry attempts on Spark Delta merge failures |
| `SPARK_CLIENT_WRITE_RETRY_DELAY` | `10` | Base delay (seconds) between Spark write retries |
