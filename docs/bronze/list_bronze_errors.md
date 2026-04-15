# list_bronze_errors

## Purpose

`ListBronzeErrorsJob` generates a comprehensive JSON report of all validation-error Parquet files produced during bronze processing. It cross-references two sources of truth -- the parent metadata files and the actual S3 object listing -- to identify errors that are recorded in metadata, errors that exist only in storage (orphans), and metadata references to files that no longer exist. This is an observability and debugging tool for the bronze-to-silver pipeline.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip list-bronze-errors` |


## How It Works

The job is self-contained in the job module (no separate core processor). The main logic lives in `ListBronzeErrorsJob.collect()`:

1. **List error objects**: Scans `archived_bronze/` in S3 for objects matching the pattern `/errors/<name>.parquet`. Collects all matching paths into a set.

2. **Scan metadata files**: Lists all `metadata.json` files under `archived_bronze/`. For each:
   - Loads the `BronzeToSilverProcessingMetadata` JSON.
   - Creates a "parent shell" dict capturing the run's key attributes: `data_source`, `run_id`, completion status, metrics, etc.
   - For each path in `metadata.errors_s3_path`, records whether the object actually exists in S3 and whether it appeared in the object listing.

3. **Attach listing-only paths**: For error objects found in the S3 listing but not recorded in any metadata:
   - Infers the parent metadata path from the directory structure.
   - If a matching parent exists, attaches the error path to that parent's report entry.
   - Otherwise, adds it to the `listing_orphans` list (errors with no parent metadata at all).

4. **Generate summary**: Computes aggregate counts:
   - Total parent runs, metadata files scanned, read failures.
   - Distinct error paths, breakdown by source (metadata vs listing-only).
   - Orphan count, metadata-referenced-but-missing-in-store count.

5. **Output**: Prints the full payload as indented JSON and returns it. The static method `get_errors_to_process` can extract just the actionable error paths (objects that exist in S3).


## Metadata

This job **reads** `BronzeToSilverProcessingMetadata` but does not create or modify any metadata. Its output is a JSON report printed to stdout.

### Report Schema

```json
{
  "generated_at": "ISO timestamp",
  "summary": {
    "parent_runs_in_report": 0,
    "metadata_json_files_scanned": 0,
    "metadata_json_read_failures": 0,
    "distinct_error_object_paths": 0,
    "error_path_rows_from_metadata": 0,
    "error_path_rows_from_listing_only_attached_to_parent": 0,
    "listing_orphans_no_parent_metadata": 0,
    "paths_recorded_in_metadata_but_missing_in_store": 0
  },
  "metadata_read_errors": [],
  "parents": { "<metadata_s3_path>": { "...parent details + error_paths..." } },
  "listing_orphans": []
}
```


This job provides a "reconciliation report" that catches several classes of issues:

- **Metadata says errors exist, but the files are gone**: Suggests premature cleanup or storage issues.
- **Error files exist in S3, but no metadata references them**: Suggests metadata was not saved (crash during processing) or was manually deleted.
- **Orphan errors with no parent metadata at all**: Suggests the parent metadata file is missing or corrupted.

By cross-referencing metadata and listing, the report gives operators a complete picture of error state without requiring them to manually browse S3 or parse metadata files.
