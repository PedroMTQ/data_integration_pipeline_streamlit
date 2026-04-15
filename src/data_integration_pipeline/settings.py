import os
import re
import tomllib
from importlib.metadata import version

os.environ['PYDANTIC_ERRORS_INCLUDE_URL'] = '0'


# -----------------------------------------------------------------------------
# App identity & version
# -----------------------------------------------------------------------------
DEBUG = int(os.getenv('DEBUG', '0'))
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
PYPROJECT_PATH = os.path.join(ROOT, 'pyproject.toml')

with open(PYPROJECT_PATH, 'rb') as f:
    _pyproject = tomllib.load(f)
    SERVICE_NAME = _pyproject['project']['name']

__VERSION__ = version(SERVICE_NAME)
CODE_VERSION = __VERSION__

# -----------------------------------------------------------------------------
# Paths (project layout)
# -----------------------------------------------------------------------------
TEMP = os.path.join(ROOT, 'tmp')
TESTS = os.path.join(ROOT, 'tests')
TESTS_DATA = os.path.join(TESTS, 'data')

# -----------------------------------------------------------------------------
# Data layer folders (pipeline stages)
# -----------------------------------------------------------------------------
BRONZE_DATA_FOLDER = os.getenv('BRONZE_DATA_FOLDER', 'bronze')
ARCHIVED_BRONZE_DATA_FOLDER = 'archived_bronze'
CHUNKED_BRONZE_DATA_FOLDER = 'chunked_bronze'
SILVER_DATA_FOLDER = 'silver'
GOLD_DATA_FOLDER = 'gold'

DATA_BUCKET = os.getenv('DATA_BUCKET')

# -----------------------------------------------------------------------------
# Storage (cloud / object store)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# File formats & standard file names
# -----------------------------------------------------------------------------
DELTA_TABLE_SUFFIX = '.delta'
CSV_SUFFIX = '.csv'
PARQUET_SUFFIX = '.parquet'
JSON_SUFFIX = '.json'
JSONL_SUFFIX = '.jsonl'
PROCESSED_CHUNK_SUFFIX = '_processed'
ERRORS_PREFIX = 'errors_'
METADATA_FILE_NAME = 'metadata.json'
BRONZE_DATA_FILE_PATTERN = re.compile(r'\.(json)|(parquet)$', re.IGNORECASE)
METADATA_FILE_PATTERN = re.compile(r'metadata\.json$', re.IGNORECASE)

LOG_DELAY = int(os.getenv('LOG_DELAY', '60'))
# -----------------------------------------------------------------------------
# Schema / column naming
# -----------------------------------------------------------------------------
HASH_DIFF_COLUMN = os.getenv('HASH_DIFF_COLUMN', 'hdiff')
LOAD_LDTS_COLUMN = os.getenv('LOAD_LDTS_COLUMN', 'load_ldts')
SYNC_LDTS_COLUMN = os.getenv('SYNC_LDTS_COLUMN', 'sync_ldts')
EMBEDDING_LDTS_COLUMN = os.getenv('EMBEDDING_LDTS_COLUMN', 'embedding_ldts')
UNKNOWN_PARTITION_STR = 'UNKNOWN'


# -----------------------------------------------------------------------------
# Batching, Delta & Processing
# -----------------------------------------------------------------------------
DEFAULT_CHUNK_SIZE = int(os.getenv('DEFAULT_CHUNK_SIZE', '10000'))
DELTA_CLIENT_BATCH_SIZE = int(os.getenv('DELTA_CLIENT_BATCH_SIZE', '100000'))

DELTA_OPTIMIZE_TARGET_SIZE = int(os.getenv('DELTA_OPTIMIZE_TARGET_SIZE', str(100 * 1024 * 1024)))  # 100mb
DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS = int(os.getenv('DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS', str(os.cpu_count() or 1)))
DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS = int(os.getenv('DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS', '60'))
DELTA_OPTIMIZE_Z_ORDER_COLUMNS = [item.strip() for item in os.getenv('DELTA_OPTIMIZE_Z_ORDER_COLUMNS', '').split(',') if item.strip()] or None

DELTA_VACUUM_RETENTION_HOURS = int(os.getenv('DELTA_VACUUM_RETENTION_HOURS', '168'))


# -----------------------------------------------------------------------------
# Audit
# -----------------------------------------------------------------------------
AUDIT_TOTAL_ROWS = int(os.getenv('AUDIT_TOTAL_ROWS', '1000'))

EMBEDDING_DIMENSIONS = int(os.getenv('EMBEDDING_DIMENSIONS', '256'))
EMBEDDING_MODEL_NAME = os.getenv('EMBEDDING_MODEL_NAME', 'jinaai/jina-embeddings-v5-text-nano')
SYNC_METADATA_FREQUENCY = int(os.environ.get('SYNC_METADATA_FREQUENCY', '600'))  # 10 minutes

# -----------------------------------------------------------------------------
# Connections
# -----------------------------------------------------------------------------

# Postgres
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_DATABASE = os.environ.get('POSTGRES_DATABASE')

# ES
ELASTICSEARCH_URL: str = os.environ.get('ELASTICSEARCH_URL')
ELASTICSEARCH_USER: str = os.environ.get('ELASTICSEARCH_USER')
ELASTICSEARCH_PASSWORD: str = os.environ.get('ELASTICSEARCH_PASSWORD')
ELASTICSEARCH_INDEX_NAME: str = os.environ.get('ELASTICSEARCH_INDEX_NAME', 'integrated_records_v1')
ELASTICSEARCH_INDEX_ALIAS: str = os.environ.get('ELASTICSEARCH_INDEX_ALIAS', 'integrated_records')
ELASTICSEARCH_NUMBER_OF_SHARDS: int = int(os.environ.get('ELASTICSEARCH_NUMBER_OF_SHARDS', '1'))

# S3

S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_HOST = os.getenv('S3_HOST')
S3_PORT = os.getenv('S3_PORT')

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', f'http://{S3_HOST}:{S3_PORT}')
S3_CLIENT_KWARGS = {
    'endpoint_url': S3_ENDPOINT_URL,
}
S3_CONFIG_KWARGS = {}
STORAGE_OPTIONS = {
    'aws_access_key_id': S3_ACCESS_KEY,
    'aws_secret_access_key': S3_SECRET_ACCESS_KEY,
    'endpoint_url': S3_ENDPOINT_URL,
    'allow_http': 'true',
}

# Backend engine (silver integration join)
ER_BACKEND_ENGINE: str = os.getenv('ER_BACKEND_ENGINE', 'duckdb')

# Spark
SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'dip-spark')
SPARK_HOST = os.getenv('SPARK_HOST', '127.0.0.1')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '4g')
SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')

# -----------------------------------------------------------------------------
# Other connection settings
# -----------------------------------------------------------------------------

ES_CLIENT_BATCH_SIZE: int = int(os.environ.get('ES_CLIENT_BATCH_SIZE', '100'))
ES_CLIENT_RAISE_ERRORS: bool = bool(int(os.environ.get('ES_CLIENT_RAISE_ERRORS', '0')))
ES_MAX_RETRIES: int = int(os.environ.get('ES_MAX_RETRIES', '10'))
ES_RETRY_BACKOFF_SECONDS: float = float(os.environ.get('ES_RETRY_BACKOFF_SECONDS', '1.0'))
ES_RETRY_MAX_BACKOFF_SECONDS: float = float(os.environ.get('ES_RETRY_MAX_BACKOFF_SECONDS', '30.0'))


POSTGRES_CONNECTION_TIMEOUT = int(os.environ.get('POSTGRES_CONNECTION_TIMEOUT', '10'))
POSTGRES_CLIENT_BATCH_SIZE = int(os.environ.get('POSTGRES_CLIENT_BATCH_SIZE', '10000'))
POSTGRES_MAX_RETRIES: int = int(os.environ.get('POSTGRES_MAX_RETRIES', '10'))
POSTGRES_RETRY_BACKOFF_SECONDS: float = float(os.environ.get('POSTGRES_RETRY_BACKOFF_SECONDS', '1.0'))
POSTGRES_RETRY_MAX_BACKOFF_SECONDS: float = float(os.environ.get('POSTGRES_RETRY_MAX_BACKOFF_SECONDS', '30.0'))
POSTGRES_RETRY_JITTER_MIN: float = float(os.environ.get('POSTGRES_RETRY_JITTER_MIN', '0.0'))
POSTGRES_RETRY_JITTER_MAX: float = float(os.environ.get('POSTGRES_RETRY_JITTER_MAX', '5.0'))
POSTGRES_RETRY_JITTER: tuple[float, float] = (POSTGRES_RETRY_JITTER_MIN, POSTGRES_RETRY_JITTER_MAX)
POSTGRES_RETRY_DELAY: float = float(os.environ.get('POSTGRES_RETRY_DELAY', '1.0'))

DELTA_CLIENT_WRITE_RETRIES = int(os.getenv('DELTA_CLIENT_WRITE_RETRIES', '5'))
DELTA_CLIENT_WRITE_RETRY_DELAY = int(os.getenv('DELTA_CLIENT_WRITE_RETRY_DELAY', '10'))
DELTA_CLIENT_WRITE_RETRY_BACKOFF = int(os.getenv('DELTA_CLIENT_WRITE_RETRY_BACKOFF', '2'))
DELTA_CLIENT_WRITE_RETRY_MAX_DELAY = int(os.getenv('DELTA_CLIENT_WRITE_RETRY_MAX_DELAY', '180'))
DELTA_CLIENT_WRITE_RETRY_JITTER_MIN = int(os.getenv('DELTA_CLIENT_WRITE_RETRY_JITTER_MIN', '0'))
DELTA_CLIENT_WRITE_RETRY_JITTER_MAX = int(os.getenv('DELTA_CLIENT_WRITE_RETRY_JITTER_MAX', '10'))
DELTA_CLIENT_WRITE_RETRY_JITTER = (DELTA_CLIENT_WRITE_RETRY_JITTER_MIN, DELTA_CLIENT_WRITE_RETRY_JITTER_MAX)

SPARK_CLIENT_WRITE_RETRIES = int(os.getenv('SPARK_CLIENT_WRITE_RETRIES', '5'))
SPARK_CLIENT_WRITE_RETRY_DELAY = int(os.getenv('SPARK_CLIENT_WRITE_RETRY_DELAY', '10'))
SPARK_CLIENT_WRITE_RETRY_BACKOFF = int(os.getenv('SPARK_CLIENT_WRITE_RETRY_BACKOFF', '2'))
SPARK_CLIENT_WRITE_RETRY_MAX_DELAY = int(os.getenv('SPARK_CLIENT_WRITE_RETRY_MAX_DELAY', '180'))
SPARK_CLIENT_WRITE_RETRY_JITTER_MIN = int(os.getenv('SPARK_CLIENT_WRITE_RETRY_JITTER_MIN', '0'))
SPARK_CLIENT_WRITE_RETRY_JITTER_MAX = int(os.getenv('SPARK_CLIENT_WRITE_RETRY_JITTER_MAX', '10'))
SPARK_CLIENT_WRITE_RETRY_JITTER = (SPARK_CLIENT_WRITE_RETRY_JITTER_MIN, SPARK_CLIENT_WRITE_RETRY_JITTER_MAX)


STREAMLIT_CACHE_TTL = int(os.getenv('STREAMLIT_CACHE_TTL', '300'))
