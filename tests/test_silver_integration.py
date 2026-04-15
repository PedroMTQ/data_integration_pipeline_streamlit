"""Integration tests for the Silver layer: integrate + audit.

Requires MinIO (and optionally Spark) to be running.
These tests depend on the bronze layer having already been loaded,
so they run the full bronze flow first.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from data_integration_pipeline.settings import SILVER_DATA_FOLDER

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_bronze_pipeline(storage_client, sample_data_dir: Path):
    """Run the full bronze flow: upload → chunk → process → load."""
    from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
    from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
    from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob
    from data_integration_pipeline.settings import BRONZE_DATA_FOLDER

    for source_dir in sorted(sample_data_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        for file_path in sorted(source_dir.iterdir()):
            s3_path = os.path.join(BRONZE_DATA_FOLDER, source_dir.name, file_path.name)
            storage_client.upload_file(str(file_path), s3_path)

    ChunkBronzeJob().run()
    ProcessBronzeJob().run()
    LoadBronzeJob().run()


def _cleanup_all(storage_client):
    from data_integration_pipeline.settings import BRONZE_DATA_FOLDER, CHUNKED_BRONZE_DATA_FOLDER

    for prefix in (BRONZE_DATA_FOLDER, CHUNKED_BRONZE_DATA_FOLDER, 'archived_bronze', SILVER_DATA_FOLDER):
        try:
            for f in storage_client.get_files(prefix=prefix):
                storage_client.delete_file(f)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSilverIntegration:
    def test_integrate_joins_three_sources(self, require_s3, sample_data_dir):
        """Integrate should produce an integrated Delta table with rows from all three sources."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.common.io.delta_client import DeltaClient
        from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob

        client = CloudStorageClient()
        _cleanup_all(client)
        _run_bronze_pipeline(client, sample_data_dir)

        job = IntegrateSilverJob()
        job.run()

        delta_client = DeltaClient()
        integrated_table = IntegrateSilverJob.OUTPUT_DELTA_TABLE
        count = delta_client.get_count(integrated_table)
        assert count == 3, f'Expected 3 integrated rows (one per company), got {count}'

        schema = delta_client.get_schema(integrated_table)
        field_names = [f.name for f in schema]
        assert 'id__company' in field_names
        assert 'loc__country' in field_names
        assert 'desc__description_long' in field_names

    def test_integrate_is_idempotent(self, require_s3, sample_data_dir):
        """Running integrate twice without new data should skip (staleness check)."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.common.io.delta_client import DeltaClient
        from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob

        client = CloudStorageClient()
        _cleanup_all(client)
        _run_bronze_pipeline(client, sample_data_dir)

        job = IntegrateSilverJob()
        job.run()

        delta_client = DeltaClient()
        version_after_first = delta_client.get_current_version(IntegrateSilverJob.OUTPUT_DELTA_TABLE)

        job.run()
        version_after_second = delta_client.get_current_version(IntegrateSilverJob.OUTPUT_DELTA_TABLE)
        assert version_after_second == version_after_first, (
            f'Expected no new Delta version on second run: v{version_after_first} vs v{version_after_second}'
        )


class TestSilverAudit:
    def test_audit_runs_without_error(self, require_s3, sample_data_dir):
        """Great Expectations audit should complete without raising."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.silver.jobs.audit_silver import AuditSilverDataJob

        client = CloudStorageClient()
        _cleanup_all(client)
        _run_bronze_pipeline(client, sample_data_dir)

        job = AuditSilverDataJob()
        tasks = list(job.get_data_to_process())
        assert len(tasks) >= 3, f'Expected at least 3 Delta tables to audit, got {len(tasks)}'

        for s3_path in tasks:
            metadata = job.process_data(s3_path)
            assert metadata.end_timestamp is not None, f'Audit did not complete for {s3_path}'
