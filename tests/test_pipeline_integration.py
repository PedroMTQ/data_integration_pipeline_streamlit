"""End-to-end integration test: runs the full pipeline via the CLI layer.

Verifies that all layers (Bronze → Silver → Gold) produce expected outputs
when run sequentially. This is the closest thing to a smoke test of the
actual ``dip pipeline`` command.

Requires all Docker services (MinIO, PostgreSQL, Elasticsearch).
"""

from __future__ import annotations

import os

import pytest

from data_integration_pipeline.settings import (
    BRONZE_DATA_FOLDER,
    CHUNKED_BRONZE_DATA_FOLDER,
    SILVER_DATA_FOLDER,
)

pytestmark = pytest.mark.integration


def _cleanup_everything():
    """Reset S3, PG, and ES state for a clean run."""
    from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
    from data_integration_pipeline.gold.io.postgres_client import PostgresClient
    from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient

    storage = CloudStorageClient()
    for prefix in (BRONZE_DATA_FOLDER, CHUNKED_BRONZE_DATA_FOLDER, 'archived_bronze', SILVER_DATA_FOLDER, 'gold'):
        try:
            for f in storage.get_files(prefix=prefix):
                storage.delete_file(f)
        except Exception:
            pass

    try:
        pg = PostgresClient()
        pg.execute('DROP TABLE IF EXISTS integrated_records CASCADE')
        pg.execute('DROP MATERIALIZED VIEW IF EXISTS integrated_records_report CASCADE')
    except Exception:
        pass

    try:
        es = ElasticsearchClient()
        es.es_client.indices.delete(index='integrated_records_*', ignore_unavailable=True)
        try:
            es.es_client.indices.delete_alias(index='*', name='integrated_records', ignore_unavailable=True)
        except Exception:
            pass
    except Exception:
        pass


class TestFullPipeline:
    def test_end_to_end_pipeline(self, require_infra, sample_data_dir):
        """Run the full pipeline and verify data flows through all layers."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.common.io.delta_client import DeltaClient
        from data_integration_pipeline.gold.io.postgres_client import PostgresClient
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
        from data_integration_pipeline.gold.io.query_client import QueryClient, SearchFilters

        from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
        from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
        from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob
        from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob

        _cleanup_everything()

        storage = CloudStorageClient()

        # Upload test data
        for source_dir in sorted(sample_data_dir.iterdir()):
            if not source_dir.is_dir():
                continue
            for file_path in sorted(source_dir.iterdir()):
                s3_path = os.path.join(BRONZE_DATA_FOLDER, source_dir.name, file_path.name)
                storage.upload_file(str(file_path), s3_path)

        # --- BRONZE ---
        ChunkBronzeJob().run()
        ProcessBronzeJob().run()
        LoadBronzeJob().run()

        delta = DeltaClient()
        silver_tables = storage.get_delta_tables(prefix=SILVER_DATA_FOLDER)
        per_source_tables = [t for t in silver_tables if 'integrated' not in t]
        assert len(per_source_tables) == 3, f'Expected 3 per-source Delta tables, got {per_source_tables}'
        for table_path in per_source_tables:
            count = delta.get_count(table_path)
            assert count == 3, f'{table_path} should have 3 rows, got {count}'

        # --- SILVER ---
        IntegrateSilverJob().run()

        integrated_count = delta.get_count('silver/integrated/records.delta')
        assert integrated_count == 3, f'Expected 3 integrated rows, got {integrated_count}'

        # --- GOLD (Postgres) ---
        SyncPostgresJob().run()

        pg = PostgresClient()
        pg_count = pg.get_count('integrated_records')
        assert pg_count == 3, f'Expected 3 PG rows, got {pg_count}'

        # --- GOLD (Elasticsearch) ---
        SyncOrganizationsElasticsearchJob().run()

        es = ElasticsearchClient()
        es.es_client.indices.refresh(index='_all')
        es_count = es.count_all()
        assert es_count == 3, f'Expected 3 ES documents, got {es_count}'

        # --- QUERY LAYER ---
        query_client = QueryClient()

        all_results = query_client.search(SearchFilters(k=10))
        assert len(all_results) == 3, f'Expected 3 results from match_all, got {len(all_results)}'

        finland_results = query_client.search(
            SearchFilters(
                query_text='fraud detection',
                country='finland',
                k=5,
            )
        )
        assert len(finland_results) >= 1
        assert any(r.company_id == 'cmp_test_001' for r in finland_results)

        big_company_results = query_client.search(
            SearchFilters(
                min_employees=100,
                k=10,
            )
        )
        for r in big_company_results:
            assert r.employees_count >= 100

    def test_pipeline_handles_rerun(self, require_infra, sample_data_dir):
        """A second full run should be a no-op (idempotent) and not change record counts."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.common.io.delta_client import DeltaClient
        from data_integration_pipeline.gold.io.postgres_client import PostgresClient
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient

        from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
        from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
        from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob
        from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob

        _cleanup_everything()
        storage = CloudStorageClient()

        def _upload():
            for source_dir in sorted(sample_data_dir.iterdir()):
                if not source_dir.is_dir():
                    continue
                for file_path in sorted(source_dir.iterdir()):
                    s3_path = os.path.join(BRONZE_DATA_FOLDER, source_dir.name, file_path.name)
                    storage.upload_file(str(file_path), s3_path)

        def _run_pipeline():
            _upload()
            ChunkBronzeJob().run()
            ProcessBronzeJob().run()
            LoadBronzeJob().run()
            IntegrateSilverJob().run()
            SyncPostgresJob().run()
            SyncOrganizationsElasticsearchJob().run()

        _run_pipeline()

        delta = DeltaClient()
        pg = PostgresClient()
        es = ElasticsearchClient()

        first_delta_count = delta.get_count('silver/integrated/records.delta')
        first_pg_count = pg.get_count('integrated_records')
        es.es_client.indices.refresh(index='_all')
        first_es_count = es.count_all()

        _run_pipeline()

        second_delta_count = delta.get_count('silver/integrated/records.delta')
        second_pg_count = pg.get_count('integrated_records')
        es.es_client.indices.refresh(index='_all')
        second_es_count = es.count_all()

        assert second_delta_count == first_delta_count, f'Delta count changed: {first_delta_count} → {second_delta_count}'
        assert second_pg_count == first_pg_count, f'PG count changed: {first_pg_count} → {second_pg_count}'
        assert second_es_count == first_es_count, f'ES count changed: {first_es_count} → {second_es_count}'
