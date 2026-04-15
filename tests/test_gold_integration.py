"""Integration tests for the Gold layer: sync PG, sync ES, and QueryClient.

Requires MinIO, PostgreSQL, and Elasticsearch to be running.
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


def _run_full_bronze_and_silver(storage_client, sample_data_dir: Path):
    """Run bronze (upload → chunk → process → load) + silver (integrate)."""
    from data_integration_pipeline.bronze.jobs.chunk_bronze import ChunkBronzeJob
    from data_integration_pipeline.bronze.jobs.process_bronze import ProcessBronzeJob
    from data_integration_pipeline.bronze.jobs.load_bronze import LoadBronzeJob
    from data_integration_pipeline.silver.jobs.integrate_silver import IntegrateSilverJob

    for source_dir in sorted(sample_data_dir.iterdir()):
        if not source_dir.is_dir():
            continue
        for file_path in sorted(source_dir.iterdir()):
            s3_path = os.path.join(BRONZE_DATA_FOLDER, source_dir.name, file_path.name)
            storage_client.upload_file(str(file_path), s3_path)

    ChunkBronzeJob().run()
    ProcessBronzeJob().run()
    LoadBronzeJob().run()
    IntegrateSilverJob().run()


def _cleanup_all(storage_client):
    for prefix in (BRONZE_DATA_FOLDER, CHUNKED_BRONZE_DATA_FOLDER, 'archived_bronze', SILVER_DATA_FOLDER):
        try:
            for f in storage_client.get_files(prefix=prefix):
                storage_client.delete_file(f)
        except Exception:
            pass


def _cleanup_pg():
    from data_integration_pipeline.gold.io.postgres_client import PostgresClient

    try:
        client = PostgresClient()
        client.execute('DROP TABLE IF EXISTS integrated_records CASCADE')
        client.execute('DROP MATERIALIZED VIEW IF EXISTS integrated_records_report CASCADE')
    except Exception:
        pass


def _cleanup_es():
    from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient

    try:
        client = ElasticsearchClient()
        client.es_client.indices.delete(index='integrated_records_*', ignore_unavailable=True)
        try:
            client.es_client.indices.delete_alias(index='*', name='integrated_records', ignore_unavailable=True)
        except Exception:
            pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSyncPostgres:
    def test_sync_pg_creates_records(self, require_infra, sample_data_dir):
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.gold.io.postgres_client import PostgresClient
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob

        client = CloudStorageClient()
        _cleanup_all(client)
        _cleanup_pg()
        _run_full_bronze_and_silver(client, sample_data_dir)

        SyncPostgresJob().run()

        pg_client = PostgresClient()
        count = pg_client.get_count('integrated_records')
        assert count == 3, f'Expected 3 rows in PG integrated_records, got {count}'

    def test_sync_pg_is_idempotent(self, require_infra, sample_data_dir):
        """Syncing twice should not duplicate records."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.gold.io.postgres_client import PostgresClient
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob

        client = CloudStorageClient()
        _cleanup_all(client)
        _cleanup_pg()
        _run_full_bronze_and_silver(client, sample_data_dir)

        SyncPostgresJob().run()
        pg_client = PostgresClient()
        count_first = pg_client.get_count('integrated_records')

        SyncPostgresJob().run()
        count_second = pg_client.get_count('integrated_records')
        assert count_second == count_first, f'Expected idempotent PG sync: {count_first} rows after first, {count_second} after second'


class TestSyncElasticsearch:
    def test_sync_es_indexes_records(self, require_infra, sample_data_dir):
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob

        client = CloudStorageClient()
        _cleanup_all(client)
        _cleanup_pg()
        _cleanup_es()
        _run_full_bronze_and_silver(client, sample_data_dir)

        SyncPostgresJob().run()
        SyncOrganizationsElasticsearchJob().run()

        es_client = ElasticsearchClient()
        es_client.es_client.indices.refresh(index='_all')
        count = es_client.count_all()
        assert count == 3, f'Expected 3 documents in ES, got {count}'


class TestQueryClient:
    def test_bm25_text_search(self, require_infra, sample_data_dir):
        """BM25 search for 'fraud detection' should return the Finnish fintech company."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob
        from data_integration_pipeline.gold.io.query_client import QueryClient, SearchFilters

        client = CloudStorageClient()
        _cleanup_all(client)
        _cleanup_pg()
        _cleanup_es()
        _run_full_bronze_and_silver(client, sample_data_dir)

        SyncPostgresJob().run()
        SyncOrganizationsElasticsearchJob().run()

        es = ElasticsearchClient()
        es.es_client.indices.refresh(index='_all')

        query_client = QueryClient()
        results = query_client.search(
            SearchFilters(
                query_text='fraud detection banking analytics',
                country='finland',
                k=3,
            )
        )
        assert len(results) >= 1, 'Expected at least one result for fraud detection query'
        company_ids = [r.company_id for r in results]
        assert 'cmp_test_001' in company_ids, f'Expected cmp_test_001 in results, got {company_ids}'

    def test_filter_only_search(self, require_infra, sample_data_dir):
        """Filter-only search by country should return matching records."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob
        from data_integration_pipeline.gold.io.query_client import QueryClient, SearchFilters

        client = CloudStorageClient()
        _cleanup_all(client)
        _cleanup_pg()
        _cleanup_es()
        _run_full_bronze_and_silver(client, sample_data_dir)

        SyncPostgresJob().run()
        SyncOrganizationsElasticsearchJob().run()

        es = ElasticsearchClient()
        es.es_client.indices.refresh(index='_all')

        query_client = QueryClient()
        results = query_client.search(
            SearchFilters(
                country='germany',
                k=10,
            )
        )
        assert len(results) >= 1
        for r in results:
            assert r.country.lower() == 'germany', f'Expected germany, got {r.country}'

    def test_numeric_range_filter(self, require_infra, sample_data_dir):
        """Filtering by min_employees should narrow results."""
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob
        from data_integration_pipeline.gold.io.query_client import QueryClient, SearchFilters

        client = CloudStorageClient()
        _cleanup_all(client)
        _cleanup_pg()
        _cleanup_es()
        _run_full_bronze_and_silver(client, sample_data_dir)

        SyncPostgresJob().run()
        SyncOrganizationsElasticsearchJob().run()

        es = ElasticsearchClient()
        es.es_client.indices.refresh(index='_all')

        query_client = QueryClient()
        results = query_client.search(
            SearchFilters(
                min_employees=200,
                k=10,
            )
        )
        assert len(results) >= 1
        for r in results:
            assert r.employees_count >= 200, f'Expected >=200 employees, got {r.employees_count}'
