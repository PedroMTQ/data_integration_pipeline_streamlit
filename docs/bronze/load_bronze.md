# load_bronze

## Purpose

`LoadBronzeJob` is the final step in the bronze-to-silver pipeline. It reads the validated and processed Parquet chunks (produced by `process_bronze`), writes them into silver-layer Delta tables, aggregates metrics from all chunks back into the parent metadata, and cleans up intermediate artifacts. After this job completes, the data is queryable as a Delta table.

## Entrypoint


| Method  | Command                                                                                                                           |
| ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| CLI     | `dip load-bronze`                                                                                                                 |
| Airflow | `get_tasks()` returns one dict per parent metadata with pending chunks; `process_task(task_dict)` loads all chunks for one parent |


## How It Works

### Job layer (`LoadBronzeJob`)

1. **Discovery (`get_data_to_process`)**: Scans `chunked_bronze/` for chunk-level `metadata.json` files. Groups chunk metadata paths by their `parent_metadata_s3_path`.
  For each parent:
  - Validates that both raw and processed chunks exist (warns on missing artifacts indicating processing errors).
  - Skips parents whose metadata already has `end_timestamp` set (completed runs -- also cleans up stale chunk metadata files).
  - Deduplicates by `output_delta_table` to prevent concurrent writes to the same Delta table.
  - Yields `{'parent_metadata_s3_path': ..., 'chunk_metadata_s3_paths': [...]}`.
2. **Processing (`process_data`)**: Loads the parent and all chunk metadata from S3, instantiates `BronzeLoader`, and calls `loader.run()`.

### Core layer (`BronzeLoader`)

The `BronzeLoader` class in `bronze/core/bronze_loader.py` handles the actual Delta writes:

1. **Batched streaming (`_stream_chunks_to_delta`)**: Iterates over all chunk metadata objects. For each:
  - Opens the processed Parquet file via `CloudFileReader(as_table=True, chunk_size=DELTA_CLIENT_BATCH_SIZE)`.
  - Accumulates `pyarrow.Table` fragments into a buffer until `DELTA_CLIENT_BATCH_SIZE` rows are reached.
  - Yields the concatenated batch for Delta writing.
  - After each yielded batch, cleans up the chunk metadata and processed files for the chunks that contributed to that batch.
   This design minimizes the number of Delta write operations by aggregating small chunks into larger batches.
2. **Delta writes (`_write_chunks_to_delta`)**: Each yielded batch is written via `DeltaClient.write()` with:
  - `primary_key`: Used for merge/upsert semantics (resolved via `ModelMapper.get_primary_key`).
  - `partition_key`: Used for Delta partitioning (resolved via `ModelMapper.get_partition_key`).
3. **Chunk cleanup (`_cleanup_chunk_metadata`)**: After a chunk's data has been written to Delta:
  - Appends the processed chunk path to `parent_metadata.processed_chunks`.
  - Joins the chunk's metrics into the parent's aggregates.
  - Appends any error paths from the chunk to the parent's `errors_s3_path` list.
  - Saves the parent metadata (incremental progress persistence).
  - Deletes the processed Parquet file and chunk metadata JSON from S3.
4. **Finalization (`_finalize_parent_metadata`)**: After all chunks are processed, checks whether any raw or processed chunks remain:
  - If none remain: marks the parent metadata as complete (`end_timestamp` is set).
  - If some remain: saves without completing (allows a subsequent run to pick up where this one left off).

### Source Files

- Job: `src/data_integration_pipeline/bronze/jobs/load_bronze.py`
- Core: `src/data_integration_pipeline/bronze/core/bronze_loader.py`
- Metadata: `src/data_integration_pipeline/bronze/core/metadata/bronze_to_silver_processing_metadata.py`

## Metadata

This job operates on two metadata models. `ChunkProcessingMetadata` is produced by `process_bronze` and consumed here; `BronzeToSilverProcessingMetadata` is the parent that accumulates results across all chunks.

**Model**: `ChunkProcessingMetadata`

| Field                      | Description                                                    |
| -------------------------- | -------------------------------------------------------------- |
| `parent_metadata_s3_path`  | S3 path of the parent `BronzeToSilverProcessingMetadata`       |
| `data_source`              | Data source identifier                                         |
| `input_chunk_s3_path`      | S3 path of the raw chunk being processed                       |
| `output_processed_s3_path` | Computed: replaces `.parquet` suffix with `_processed.parquet`  |
| `errors_s3_path`           | S3 path of error rows (set only if failures occurred)          |
| `metrics`                  | Success/failure counts                                         |

**Model**: `BronzeToSilverProcessingMetadata`

| Field                | Description                                                                  |
| -------------------- | ---------------------------------------------------------------------------- |
| `input_raw_file`     | Original raw file path (e.g. `bronze/dataset_1_generated.json`)              |
| `data_source`        | Data source identifier (resolved from `ModelMapper`)                         |
| `chunks_size`        | Configured chunk size used during the chunking step                          |
| `bucket_name`        | S3 bucket name                                                               |
| `archived_raw_file`  | S3 path of the archived raw file                                             |
| `raw_chunks`         | List of S3 paths for raw chunk Parquet files                                 |
| `processed_chunks`   | List of S3 paths for processed chunks (populated incrementally during load)  |
| `errors_s3_path`     | List of S3 paths to error Parquet files (aggregated from chunk metadata)     |
| `requeued_raw_file`  | S3 path if the raw file was requeued for reprocessing                        |
| `output_delta_table` | Computed: target Delta table path (e.g. `silver/dataset_1/records.delta`)    |
| `n_chunks`           | Computed: count of raw chunks                                                |
| `metrics`            | Aggregated success/failure counts across all chunks                          |

## Variables

| Variable | Default | Description |
|---|---|---|
| `DELTA_CLIENT_BATCH_SIZE` | `100000` | Rows accumulated from processed chunks before flushing a single Delta write |
| `DELTA_CLIENT_WRITE_RETRIES` | `5` | Number of retry attempts on Delta merge/write failures |
| `DELTA_CLIENT_WRITE_RETRY_DELAY` | `10` | Base delay (seconds) between Delta write retries |
| `DELTA_CLIENT_WRITE_RETRY_BACKOFF` | `2` | Exponential backoff multiplier for Delta write retries |
| `DELTA_CLIENT_WRITE_RETRY_MAX_DELAY` | `180` | Maximum delay (seconds) between Delta write retries |
