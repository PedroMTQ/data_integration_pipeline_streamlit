# chunk_bronze

## Purpose

`ChunkBronzeJob` splits large raw bronze files (JSON or Parquet) into smaller Parquet chunks and archives the originals. This is the first processing step after upload -- it converts heterogeneous raw formats into a uniform chunked Parquet layout and creates the `BronzeToSilverProcessingMetadata` that tracks the entire bronze-to-silver lifecycle for each input file.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip chunk-bronze` |
| Airflow | `get_tasks()` returns one dict per raw file; `process_task(task_dict)` chunks a single file |

## How It Works

### Job layer (`ChunkBronzeJob`)

1. **Discovery (`get_data_to_process`)**: Scans the `bronze/` S3 prefix for files matching `BRONZE_DATA_FILE_PATTERN` (`.json` or `.parquet`). Each match yields a `{'bronze_s3_path': ...}` task dict.

2. **Processing (`process_data`)**: For each raw file:
   - Creates a `BronzeToSilverProcessingMetadata` via its `.create()` factory, which resolves the data source through `ModelMapper`, generates a unique `run_id`, and computes the `archived_raw_file` path.
   - Instantiates a `Chunker` with that metadata and calls `chunker.run()`.

3. **Output**: Returns the metadata object (now populated with the list of chunk paths).

### Core layer (`Chunker`)

The `Chunker` class in `bronze/core/bronze_chunker.py` performs the actual I/O:

1. Opens the raw file via `CloudFileReader` with `chunk_size=DEFAULT_CHUNK_SIZE` (default 10,000 rows). The reader supports both JSON and Parquet formats and yields `pyarrow.Table` chunks via a context-manager streaming interface.

2. For each chunk:
   - Constructs the output path: `chunked_bronze/<data_source>/<run_id>/chunk_<N>.parquet`.
   - Writes the chunk as Parquet via `CloudFileWriter`.
   - Appends the chunk path to `metadata.raw_chunks`.

3. After all chunks are written, **moves** (*not* copies) the original raw file to its archived location (`archived_bronze/<data_source>/<run_id>/<original_filename>`). This ensures the file is not re-discovered on the next run.

4. Persists the metadata JSON to S3 at the archive location.

## Metadata

**Model**: `BronzeToSilverProcessingMetadata` (extends `BaseMetadata`)

| Field | Description |
|---|---|
| `input_raw_file` | Original S3 path of the raw file |
| `data_source` | Resolved data source identifier (e.g. `dataset_1`) |
| `chunks_size` | Configured chunk size (from `DEFAULT_CHUNK_SIZE`) |
| `archived_raw_file` | S3 path where the original is moved after chunking |
| `raw_chunks` | List of S3 paths for the produced Parquet chunks |
| `processed_chunks` | Populated later by `load_bronze` |
| `errors_s3_path` | Populated later by `process_bronze` |
| `output_delta_table` | Computed field: `silver/<data_source>/records.delta` |
| `metrics` | Aggregated success/failure counts (populated during processing) |

**S3 path**: `archived_bronze/<data_source>/<run_id>/metadata.json`

This metadata object is the central coordination artifact for the entire bronze-to-silver flow. It is created here, updated by `process_bronze` and `load_bronze`, and marked complete when all chunks have been processed and then loaded into the respective Delta table.

