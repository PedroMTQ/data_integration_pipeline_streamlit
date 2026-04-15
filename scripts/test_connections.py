"""Test connectivity to Postgres, CE Postgres, Cloud Storage, and Elasticsearch."""

import sys

import psycopg
import urllib3
from elasticsearch import Elasticsearch

from data_integration_pipeline.settings import (
    POSTGRES_CONNECTION_TIMEOUT,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_USER,
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    DATA_BUCKET,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GREEN = '\033[92m'
RED = '\033[91m'
BOLD = '\033[1m'
RESET = '\033[0m'


def _pass(label: str, detail: str = '') -> bool:
    suffix = f' ({detail})' if detail else ''
    print(f'  {GREEN}PASS{RESET}  {label}{suffix}')
    return True


def _fail(label: str, error: Exception) -> bool:
    print(f'  {RED}FAIL{RESET}  {label} -- {error}')
    return False


def test_postgres() -> bool:
    label = 'Postgres'
    if not POSTGRES_HOST:
        return _fail(label, ValueError('POSTGRES_HOST is not set'))
    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DATABASE,
            connect_timeout=POSTGRES_CONNECTION_TIMEOUT,
            autocommit=True,
        )
        conn.execute('SELECT 1')
        conn.close()
        return _pass(label, f'{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}')
    except Exception as e:
        return _fail(label, e)


def test_cloud_storage() -> bool:
    label = 'Cloud Storage s3)'
    if not DATA_BUCKET:
        return _fail(label, ValueError('DATA_BUCKET is not set'))
    try:
        from data_integration_pipeline.common.io.cloud_storage.storage_client import CloudStorageClient

        CloudStorageClient(bucket_name=DATA_BUCKET)
        return _pass(label, f'bucket={DATA_BUCKET}')
    except Exception as e:
        return _fail(label, e)


def test_elasticsearch() -> bool:
    label = 'Elasticsearch'
    if not ELASTICSEARCH_URL:
        return _fail(label, ValueError('ELASTICSEARCH_URL is not set'))
    try:
        auth = None
        if ELASTICSEARCH_USER and ELASTICSEARCH_PASSWORD:
            auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
        es = Elasticsearch([ELASTICSEARCH_URL], basic_auth=auth, verify_certs=False)
        if not es.ping():
            return _fail(label, ConnectionError('ping returned False'))
        info = es.info()
        version = info['version']['number']
        es.close()
        return _pass(label, f'{ELASTICSEARCH_URL} (v{version})')
    except Exception as e:
        return _fail(label, e)


def main() -> int:
    print(f'\n{BOLD}Testing connections...{RESET}\n')

    results = [
        test_postgres(),
        test_cloud_storage(),
        test_elasticsearch(),
    ]

    passed = sum(results)
    total = len(results)
    print()

    if passed == total:
        print(f'{GREEN}{BOLD}All {total} connections OK{RESET}\n')
        return 0

    print(f'{RED}{BOLD}{total - passed}/{total} connections failed{RESET}\n')
    return 1


if __name__ == '__main__':
    sys.exit(main())
