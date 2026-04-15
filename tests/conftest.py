"""Shared fixtures for integration tests.

These tests require Docker infrastructure (MinIO, PostgreSQL, Elasticsearch)
to be running. Start them with ``make docker-infra-up`` before running tests.

All fixtures use a dedicated S3 prefix (``test_integration/``) to avoid
colliding with real pipeline data, and clean it up after the session.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_integration_pipeline.settings import EMBEDDING_DIMENSIONS


# ---------------------------------------------------------------------------
# Pytest markers
# ---------------------------------------------------------------------------


def pytest_configure(config):
    config.addinivalue_line('markers', 'integration: requires Docker services (MinIO, PG, ES)')


# ---------------------------------------------------------------------------
# Service availability checks
# ---------------------------------------------------------------------------


def _s3_available() -> bool:
    try:
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient

        client = CloudStorageClient()
        client.test_connection()
        return True
    except Exception:
        return False


def _pg_available() -> bool:
    try:
        from data_integration_pipeline.gold.io.postgres_client import PostgresClient

        client = PostgresClient()
        client.ping()
        return True
    except Exception:
        return False


def _es_available() -> bool:
    try:
        from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient

        client = ElasticsearchClient()
        return client.ping()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Session-scoped skip guards
# ---------------------------------------------------------------------------


@pytest.fixture(scope='session')
def require_s3():
    if not _s3_available():
        pytest.skip('MinIO/S3 is not available — start with `make docker-infra-up`')


@pytest.fixture(scope='session')
def require_pg():
    if not _pg_available():
        pytest.skip('PostgreSQL is not available — start with `make docker-infra-up`')


@pytest.fixture(scope='session')
def require_es():
    if not _es_available():
        pytest.skip('Elasticsearch is not available — start with `make docker-infra-up`')


@pytest.fixture(scope='session')
def require_infra(require_s3, require_pg, require_es):
    """Convenience: skip if any service is missing."""


# ---------------------------------------------------------------------------
# Sample data generators
# ---------------------------------------------------------------------------

SAMPLE_COMPANIES = [
    {
        'company_id': 'cmp_test_001',
        'country': 'Finland',
        'industry': 'Fintech',
        'revenue_range': '100M-500M',
        'employee_count': 240,
        'founded_year': 2018,
        'short_description': 'Nordic fintech solutions for banking',
        'website_url': 'www.nordic-fintech-solutions.ai',
    },
    {
        'company_id': 'cmp_test_002',
        'country': 'Germany',
        'industry': 'Healthcare',
        'revenue_range': '10M-50M',
        'employee_count': 85,
        'founded_year': 2020,
        'short_description': 'AI diagnostics platform',
        'website_url': 'https://health-ai-diagnostics.de',
    },
    {
        'company_id': 'cmp_test_003',
        'country': 'France',
        'industry': 'Biotech',
        'revenue_range': '500M-1B',
        'employee_count': 500,
        'founded_year': 2015,
        'short_description': None,
        'website_url': 'https://www.bio-discovery-france.com',
    },
]

SAMPLE_ENRICHMENTS = [
    {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection and digital banking.',
        'product_services': 'AI underwriting\nFraud detection platform\nBanking analytics dashboards',
        'market_niches': 'Digital Banking\nPayments\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    },
    {
        'url': 'https://health-ai-diagnostics.de',
        'business_description': 'AI-powered diagnostics and patient monitoring for hospitals.',
        'product_services': 'Diagnostic imaging\nPatient monitoring\nClinical analytics',
        'market_niches': 'Healthcare\nDiagnostics\nGermany',
        'business_model': 'B2B | licensing | SaaS',
    },
    {
        'url': 'https://www.bio-discovery-france.com',
        'business_description': 'Drug discovery and molecular research platform.',
        'product_services': 'Drug discovery\nMolecular simulation\nClinical trial management',
        'market_niches': 'Biotech\nPharmaceuticals\nFrance',
        'business_model': 'B2B | R&D contracts | licensing',
    },
]


def _fake_embedding() -> list[float]:
    """Deterministic fake embedding of the configured dimension."""
    return [float(i) / EMBEDDING_DIMENSIONS for i in range(EMBEDDING_DIMENSIONS)]


SAMPLE_EMBEDDINGS = [
    {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description_emb': _fake_embedding(),
        'product_services_emb': _fake_embedding(),
        'market_niches_emb': _fake_embedding(),
        'business_model_emb': _fake_embedding(),
        'short_description_emb': _fake_embedding(),
        'embedding_updated_at': '2024-10-21T06:52:31Z',
    },
    {
        'url': 'https://health-ai-diagnostics.de',
        'business_description_emb': _fake_embedding(),
        'product_services_emb': _fake_embedding(),
        'market_niches_emb': _fake_embedding(),
        'business_model_emb': _fake_embedding(),
        'short_description_emb': _fake_embedding(),
        'embedding_updated_at': '2024-11-01T10:00:00Z',
    },
    {
        'url': 'https://www.bio-discovery-france.com',
        'business_description_emb': _fake_embedding(),
        'product_services_emb': _fake_embedding(),
        'market_niches_emb': _fake_embedding(),
        'business_model_emb': _fake_embedding(),
        'short_description_emb': _fake_embedding(),
        'embedding_updated_at': '2024-12-15T14:30:00Z',
    },
]


@pytest.fixture(scope='session')
def sample_data_dir(tmp_path_factory) -> Path:
    """Write the three sample JSON/Parquet files to a temp directory."""
    base = tmp_path_factory.mktemp('test_bronze_data')

    ds1_dir = base / 'dataset_1'
    ds1_dir.mkdir()
    (ds1_dir / 'dataset_1_generated.json').write_text(json.dumps(SAMPLE_COMPANIES))

    ds2_dir = base / 'dataset_2'
    ds2_dir.mkdir()
    (ds2_dir / 'dataset_2_generated.json').write_text(json.dumps(SAMPLE_ENRICHMENTS))

    ds3_dir = base / 'dataset_3'
    ds3_dir.mkdir()
    table = pa.Table.from_pylist(SAMPLE_EMBEDDINGS)
    pq.write_table(table, str(ds3_dir / 'dataset_3.parquet'))

    return base


# ---------------------------------------------------------------------------
# S3 cleanup helper
# ---------------------------------------------------------------------------


@pytest.fixture(scope='session')
def s3_test_prefix() -> str:
    """Unique S3 prefix for this test session — cleaned up at teardown."""
    return 'test_integration'


@pytest.fixture(scope='session', autouse=False)
def clean_s3_prefix(require_s3, s3_test_prefix):
    """Delete any leftover test data before and after the session."""
    from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient

    client = CloudStorageClient()

    def _cleanup():
        try:
            for f in client.get_files(prefix=s3_test_prefix):
                client.delete_file(f)
        except Exception:
            pass

    _cleanup()
    yield s3_test_prefix
    _cleanup()
