# process_bronze

## Purpose

`ProcessBronzeJob` validates and transforms each raw Parquet chunk (produced by `chunk_bronze`) against its Pydantic data model, writing a processed Parquet file that conforms to the silver schema. Invalid rows are separated into error files for later analysis. This is the data quality gate between raw ingestion and the silver layer.

## Entrypoint


| Method  | Command                                                                                                  |
| ------- | -------------------------------------------------------------------------------------------------------- |
| CLI     | `dip process-bronze`                                                                                     |
| Airflow | `get_tasks()` returns one dict per unprocessed chunk; `process_task(task_dict)` processes a single chunk |


## How It Works

### Job layer (`ProcessBronzeJob`)

1. **Discovery (`get_data_to_process`)**: Scans `archived_bronze/` for `metadata.json` files. For each parent metadata:
  - Skips if `end_timestamp` is set (run already complete).
  - Iterates over `raw_chunks` and yields a task for each chunk whose S3 object still exists (chunks that have been processed are deleted, so their absence signals completion).
2. **Processing (`process_chunk`)**: For each chunk:
  - Loads the parent `BronzeToSilverProcessingMetadata` from S3.
  - Creates a `ChunkProcessingMetadata` via `.create()`.
  - Instantiates `BronzeProcessor` and calls `processor.run()`.

### Core layer (`BronzeProcessor`)

The `BronzeProcessor` class in `bronze/core/bronze_processor.py` handles the per-chunk validation and transformation:

1. **Model resolution**: Resolves the Pydantic data model for the input file via `ModelMapper.get_data_model()`. This model defines both the raw-to-silver transformation (`from_raw_record`) and the target PyArrow schema (`_pa_silver_schema`).
2. **Streaming validation**: Opens the raw chunk via `CloudFileReader(as_table=False)` to iterate row-by-row:
  - Each row is passed through `data_model.from_raw_record(raw_record=row)`, which parses, validates, and transforms the raw record into a silver-schema Pydantic model.
  - **Success**: The `.silver_record.model_dump()` dict is yielded to the output stream. Metrics are logged as success. Per-field data metrics are collected.
  - **Failure**: The raw row is written to an error Parquet file at `archived_bronze/<source>/<run_id>/errors/<chunk_name>.parquet`. Metrics are logged as failure.
3. **Batched writing**: Valid rows are collected into batches of `DELTA_CLIENT_BATCH_SIZE` and converted to a `pyarrow.Table` using the silver schema, then written to the processed output file via `CloudFileWriter`.
4. **Cleanup**: After processing:
  - If there were failures, the error path is recorded in `chunk_metadata.errors_s3_path`.
  - If there were any valid rows, chunk metadata is marked complete and persisted.
  - The raw chunk file is **deleted** (it has been consumed).
5. **Progress logging**: A `TimedTrigger` periodically logs metrics during long-running chunks (controlled by `LOG_DELAY`).


## Metadata


| Field                      | Description                                                    |
| -------------------------- | -------------------------------------------------------------- |
| `parent_metadata_s3_path`  | S3 path of the parent `BronzeToSilverProcessingMetadata`       |
| `data_source`              | Data source identifier                                         |
| `input_chunk_s3_path`      | S3 path of the raw chunk being processed                       |
| `output_processed_s3_path` | Computed: replaces `.parquet` suffix with `_processed.parquet` |
| `errors_s3_path`           | S3 path of error rows (set only if failures occurred)          |
| `metrics`                  | Success/failure counts                                         |

## Variables

| Variable | Default | Description |
|---|---|---|
| `DELTA_CLIENT_BATCH_SIZE` | `100000` | Number of valid rows accumulated before flushing a PyArrow batch to the processed Parquet file |


