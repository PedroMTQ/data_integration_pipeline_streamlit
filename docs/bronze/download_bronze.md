# download_bronze

## Purpose

`DownloadBronzeJob` bootstraps the pipeline by fetching the raw source datasets from a public Hetzner object-storage bucket and writing them to the local filesystem under `tests/data/bronze/`. This is the very first step when running the pipeline locally or in CI -- it ensures the three canonical input files exist on disk before any S3 upload or processing begins.

## Entrypoint

| Method | Command                                                           |
| ------ | ----------------------------------------------------------------- |
| CLI    | `dip download-bronze`                                             |



The download job is intentionally separated from the upload job so that the two concerns are decoupled:

- **Download** is about obtaining test data from an external source onto the local filesystem.
- **Upload** (the next step) is about pushing that data into the pipeline's S3-compatible object store.

This separation means you can re-download without re-uploading, and vice-versa. It also keeps the download logic free of any S3 client dependencies.

