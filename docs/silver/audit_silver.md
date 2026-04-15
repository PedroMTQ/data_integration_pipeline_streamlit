# audit_silver

## Purpose

`AuditSilverDataJob` runs data quality audits on all silver-layer Delta tables using Great Expectations (GX). It samples data using a weighted reservoir algorithm, applies schema-derived validation rules, and produces HTML audit reports. This job is the primary data quality visualization for the silver layer. 

*I didn't include an audit gold job, but this could be easily done.*

## Entrypoint


| Method  | Command                                                                                          |
| ------- | ------------------------------------------------------------------------------------------------ |
| CLI     | `dip audit-silver`                                                                               |
| Airflow | `get_tasks()` returns one S3 path per Delta table; `process_task(s3_path)` audits a single table |


Audit reports can be viewed via `dip audit-docs` which serves the generated HTML locally.

## How It Works

### Job layer (`AuditSilverDataJob`)

1. **Discovery (`get_data_to_process`)**: Uses `CloudStorageClient.get_delta_tables(prefix=SILVER_DATA_FOLDER)` to find all Delta tables under `silver/`. Yields each table path.
2. **Processing (`process_data`)**:
  - Creates an `AuditMetadata` with `dataset_stage='silver'`.
  - Instantiates an `AuditRunner` and calls `runner.run(metadata)`.
  - On failure, captures the error in metadata.
  - Always marks metadata as complete.

### Core layer: `AuditRunner`

The `AuditRunner` in `auditor/core/audit_runner.py` orchestrates sampling, rule assembly, and execution:

1. **Model resolution**: Resolves the data model and primary/partition keys via `ModelMapper`.
2. **Sampling**: Instantiates `DeltaWeightedDataSampler` with:
  - `weight_column`: the partition key (typically `iso2_country_code`), used to ensure geographic diversity in the sample.
  - `target_total_rows`: from `AUDIT_TOTAL_ROWS` (default 1000).
3. **Rule assembly**: Creates a `DataAuditor` with the data model, dataset stage, and additional uniqueness rules for the primary key.
4. **Data fetch**: Calls `sampler.get_filtered_data(columns_filter=data_auditor.audit_columns)` to retrieve only the columns needed for auditing.
5. **Execution**: `data_auditor.run(data=data_sample)` runs the GX validation and `data_auditor.export_docs()` generates HTML reports.
6. **Metadata enrichment**: Records sampling statistics (total raw records, sampled records, raw/sample distributions) and audit columns into the metadata.

### Core layer: `DeltaWeightedDataSampler`

The sampler in `auditor/io/delta_weighted_data_sampler.py` implements two reservoir sampling algorithms:

- **Weighted mode (A-Res)**: When `weights` are provided, uses `score = random(0,1) ^ (1/weight)` to bias the sample toward higher-weight groups without excluding rare ones.
- **Standard mode (Algorithm R)**: When no weights are provided (the default for silver audits), all records have equal probability.

Key properties:

- **Memory-efficient**: Maintains a fixed-size heap of `target_total_rows` entries regardless of stream size.
- **Streaming**: Processes data in batches from the Delta table without materializing the full dataset.
- **Distribution tracking**: Tracks per-group counts for both the raw data and the sample, enabling distribution comparison.

### Core layer: `DataAuditor`

The `DataAuditor` in `auditor/core/data_auditor.py` manages GX expectations:

1. **Expectation definitions**: Builds a list of pattern-matched rules using `fnmatch` against the PyArrow schema column names. Rules include:
  - Primary key not-null and uniqueness checks (critical severity).
  - ISO2 country code: length = 2, not-null at 98% (critical).
  - Country name: not-null at 95% (warning).
  - URL fields: not-null thresholds and regex validation (warning).
  - Employee count: range 0 to 10M (warning).
  - Founded year: range 1000 to current year + 1 (warning).
  - Revenue fields: range 0 to 10^15 (warning).
  - Description/text fields: not-null at 35% (info).
2. **Expectation binding**: Each template (`ModelExpectationTemplate`) is applied to matching columns, producing `ModelExpectation` instances that wrap actual GX expectation objects.
3. **Execution**: Converts PyArrow data to pandas (GX's pandas datasource), runs the validation suite, and processes results by severity level.
4. **Result classification**:
  - **Exception failures**: hard stop.
  - **Critical failures**: hard stop.
  - **Warning failures**: pass with warnings.
  - **Info failures**: pass with info.


## Metadata


| Field                       | Description                                                                |
| --------------------------- | -------------------------------------------------------------------------- |
| `s3_path`                   | Delta table path being audited                                             |
| `dataset_stage`             | `'bronze'`, `'silver'`, or `'gold'`                                        |
| `total_raw_records`         | Total rows in the Delta table before sampling                              |
| `total_sampled_records`     | Number of rows in the sample passed to Great Expectations                  |
| `raw_data_distribution`     | Per-partition-key value counts across the full table                       |
| `sample_data_distribution`  | Per-partition-key value counts in the sample                               |
| `audit_columns`             | List of columns that were audited                                          |
| `error_message`             | Set if the audit raised an exception; `None` on success                    |

## Variables

| Variable | Default | Description |
|---|---|---|
| `AUDIT_TOTAL_ROWS` | `1000` | Target reservoir size for the weighted sampler — controls how many rows are audited per table |



- **`fnmatch` pattern matching depends on the flat naming convention**: Expectation rules target columns by patterns like `desc__description_*` or `*revenue*`. If the silver schema changes its naming convention (e.g. drops the prefix), rules will silently stop matching and those columns will go unaudited. There is no warning for zero-match patterns.

