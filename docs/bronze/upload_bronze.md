# upload_bronze

## Purpose

`UploadBronzeJob` takes the raw dataset files that were previously downloaded to the local filesystem (by `download_bronze`) and uploads them into the pipeline's S3-compatible object store under the `bronze/` prefix. This makes the data available to all subsequent pipeline stages, which operate entirely on cloud storage.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip upload-bronze` |

Like `download_bronze`, this job does **not** expose `get_tasks` / `process_task` -- it is a one-shot local utility.

