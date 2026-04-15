"""Integration tests for the Bronze layer: upload → chunk → process → load.

Requires MinIO to be running (``make docker-infra-up``).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from data_integration_pipeline.settings import (
    BRONZE_DATA_FOLDER,
    CHUNKED_BRONZE_DATA_FOLDER,
    SILVER_DATA_FOLDER,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upload_test_files(storage_client, sample_data_dir: Path):
    """Upload test data from the temp dir to S3 bronze/."""
    for source_dir in sorted(sample_data_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        for file_path in sorted(source_dir.iterdir()):
            s3_path = os.path.join(BRONZE_DATA_FOLDER, source_dir.name, file_path.name)
            storage_client.upload_file(str(file_path), s3_path)


def _cleanup_bronze(storage_client):
    """Remove test artifacts from S3."""
    for prefix in (BRONZE_DATA_FOLDER, CHUNKED_BRONZE_DATA_FOLDER, 'archived_bronze', SILVER_DATA_FOLDER):
        try:
            for f in storage_client.get_files(prefix=prefix):
                storage_client.delete_file(f)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBronzeUpload:
    def test_upload_creates_files_in_s3(self, require_s3, sample_data_dir):
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient

        client = CloudStorageClient()
        _cleanup_bronze(client)

        _upload_test_files(client, sample_data_dir)

        files = client.get_files(prefix=BRONZE_DATA_FOLDER)
        assert len(files) == 3, f'Expected 3 files in bronze, got {len(files)}: {files}'

        ds1_files = [f for f in files if 'dataset_1' in f]
        ds2_files = [f for f in files if 'dataset_2' in f]
        ds3_files = [f for f in files if 'dataset_3' in f]
        assert len(ds1_files) == 1
        assert len(ds2_files) == 1
        assert len(ds3_files) == 1


class TestBronzeChunking:
    def test_chunk_creates_parquet_chunks(self, require_s3, sample_data_dir):
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob

        client = CloudStorageClient()
        _cleanup_bronze(client)
        _upload_test_files(client, sample_data_dir)

        job = ChunkBronzeJob()
        job.run()

        chunked_files = client.get_files(prefix=CHUNKED_BRONZE_DATA_FOLDER)
        assert len(chunked_files) > 0, 'Expected at least one chunk file'
        for f in chunked_files:
            assert f.endswith('.parquet') or f.endswith('.json'), f'Unexpected file: {f}'

        bronze_files = client.get_files(prefix=BRONZE_DATA_FOLDER)
        assert len(bronze_files) == 0, f'Original bronze files should be archived, still found: {bronze_files}'


class TestBronzeProcessing:
    def test_process_validates_and_writes_processed_chunks(self, require_s3, sample_data_dir):
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
        from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob

        client = CloudStorageClient()
        _cleanup_bronze(client)
        _upload_test_files(client, sample_data_dir)

        ChunkBronzeJob().run()
        ProcessBronzeJob().run()

        chunked_files = client.get_files(prefix=CHUNKED_BRONZE_DATA_FOLDER)
        processed_files = [f for f in chunked_files if '_processed' in f]
        assert len(processed_files) >= 3, f'Expected at least 3 processed files (one per source), got {len(processed_files)}'


class TestBronzeLoad:
    def test_load_writes_to_delta_tables(self, require_s3, sample_data_dir):
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.common.io.delta_client import DeltaClient
        from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
        from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
        from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob

        client = CloudStorageClient()
        _cleanup_bronze(client)
        _upload_test_files(client, sample_data_dir)

        ChunkBronzeJob().run()
        ProcessBronzeJob().run()
        LoadBronzeJob().run()

        delta_client = DeltaClient()
        delta_tables = client.get_delta_tables(prefix=SILVER_DATA_FOLDER)
        assert len(delta_tables) >= 3, f'Expected 3 Delta tables (one per source), got {len(delta_tables)}: {delta_tables}'

        for table_path in delta_tables:
            count = delta_client.get_count(table_path)
            assert count > 0, f'Delta table {table_path} is empty'

        ds1_table = [t for t in delta_tables if 'dataset_1' in t][0]
        ds1_count = delta_client.get_count(ds1_table)
        assert ds1_count == 3, f'Expected 3 rows in dataset_1 Delta, got {ds1_count}'

    def test_load_is_idempotent(self, require_s3, sample_data_dir):
        """Running load twice should not duplicate records (upsert semantics)."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.common.io.delta_client import DeltaClient
        from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
        from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
        from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob

        client = CloudStorageClient()
        _cleanup_bronze(client)
        _upload_test_files(client, sample_data_dir)

        ChunkBronzeJob().run()
        ProcessBronzeJob().run()
        LoadBronzeJob().run()

        delta_client = DeltaClient()
        ds1_table = f'{SILVER_DATA_FOLDER}/dataset_1/records.delta'
        count_after_first = delta_client.get_count(ds1_table)

        _upload_test_files(client, sample_data_dir)
        ChunkBronzeJob().run()
        ProcessBronzeJob().run()
        LoadBronzeJob().run()

        count_after_second = delta_client.get_count(ds1_table)
        assert count_after_second == count_after_first, (
            f'Expected idempotent load: {count_after_first} rows after first, {count_after_second} after second'
        )
