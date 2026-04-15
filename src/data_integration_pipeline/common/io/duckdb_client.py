import duckdb
from data_integration_pipeline.settings import S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL


class DuckdbClient:
    @staticmethod
    def load_delta_scan(connection: duckdb.DuckDBPyConnection):
        connection.execute('INSTALL delta; LOAD delta;')

    @staticmethod
    def load_s3_connector(connection: duckdb.DuckDBPyConnection):
        connection.execute('INSTALL httpfs; LOAD httpfs;')

    @staticmethod
    def add_s3_secret(connection: duckdb.DuckDBPyConnection, endpoint: str | None = None):
        """
        Configures DuckDB's S3 access for MinIO/S3-compatible storage.
        """
        endpoint = endpoint or S3_ENDPOINT_URL
        endpoint = endpoint.replace('http://', '').replace('https://', '')
        connection.execute(f"""
                CREATE OR REPLACE SECRET minio_secret (
                    TYPE S3,
                    PROVIDER config,
                    KEY_ID '{S3_ACCESS_KEY}',
                    SECRET '{S3_SECRET_ACCESS_KEY}',
                    REGION 'us-east-1',
                    ENDPOINT '{endpoint}',
                    URL_STYLE 'path',
                    USE_SSL 'false'
                );
            """)
