import os
from collections.abc import Iterable
from typing import Optional, Union

from data_integration_pipeline.common.core.utils import get_timestamp
import pyarrow as pa
from data_integration_pipeline.common.io.logger import logger
from py4j.protocol import Py4JJavaError
from retry import retry
from data_integration_pipeline.settings import (
    S3_ACCESS_KEY,
    S3_ENDPOINT_URL,
    S3_SECRET_ACCESS_KEY,
    SPARK_APP_NAME,
    SPARK_HOST,
    SPARK_MASTER,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
)
from data_integration_pipeline.settings import (
    DATA_BUCKET,
    HASH_DIFF_COLUMN,
    LOAD_LDTS_COLUMN,
    UNKNOWN_PARTITION_STR,
    SPARK_CLIENT_WRITE_RETRIES,
    SPARK_CLIENT_WRITE_RETRY_DELAY,
    SPARK_CLIENT_WRITE_RETRY_BACKOFF,
    SPARK_CLIENT_WRITE_RETRY_MAX_DELAY,
    SPARK_CLIENT_WRITE_RETRY_JITTER,
)
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class SparkClient:
    """
    Client to read, write, and merge Delta tables using PySpark.
    """

    # Must match the Hadoop version bundled with PySpark (check jars/hadoop-client-api-*.jar)
    _HADOOP_AWS_PACKAGES = [
        'org.apache.hadoop:hadoop-aws:3.4.2',
        'com.amazonaws:aws-java-sdk-bundle:1.12.367',
    ]

    def __init__(
        self,
        bucket_name: Optional[str] = DATA_BUCKET,
        app_name: str = SPARK_APP_NAME,
    ):
        if not bucket_name:
            raise ValueError('bucket_name or DATA_BUCKET must be set')
        self._app_name = app_name
        self.base_path = f's3a://{bucket_name}'
        self._spark = None

    @property
    def spark(self):
        if self._spark is None:
            self._spark = self._build_session()
        return self._spark

    def _build_session(self):
        endpoint = S3_ENDPOINT_URL or ''
        endpoint_stripped = endpoint.replace('http://', '').replace('https://', '')
        use_ssl = 'true' if endpoint.startswith('https') else 'false'

        builder = (
            SparkSession.builder.appName(self._app_name)
            .master(SPARK_MASTER)
            .config('spark.driver.host', SPARK_HOST)
            .config('spark.driver.memory', SPARK_DRIVER_MEMORY)
            .config('spark.executor.memory', SPARK_EXECUTOR_MEMORY)
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.sql.parquet.columnarReaderBatchSize', '1024')
            .config('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY or '')
            .config('spark.hadoop.fs.s3a.secret.key', S3_SECRET_ACCESS_KEY or '')
            .config('spark.hadoop.fs.s3a.endpoint', endpoint_stripped)
            .config('spark.hadoop.fs.s3a.path.style.access', 'true')
            .config('spark.hadoop.fs.s3a.connection.ssl.enabled', use_ssl)
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.ui.showConsoleProgress', 'false')
        )
        spark = configure_spark_with_delta_pip(builder, extra_packages=self._HADOOP_AWS_PACKAGES).getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        logger.info(f'SparkSession started: {self._app_name}')
        return spark

    def _get_uri(self, table_name: str) -> str:
        return os.path.join(self.base_path, table_name)

    # ------------------------------------------------------------------
    # _prepare_data  (mirrors DeltaClient.__prepare_data)
    # ------------------------------------------------------------------

    @staticmethod
    def __prepare_data(data: pyspark.sql.DataFrame, primary_key: Union[str, Iterable], partition_key: Optional[str] = None) -> pyspark.sql.DataFrame:
        if not primary_key:
            raise ValueError('Missing primary_key')
        now = get_timestamp(as_str=True)
        if partition_key:
            data = data.fillna({partition_key: UNKNOWN_PARTITION_STR})

        if isinstance(primary_key, str):
            exclude = {primary_key, HASH_DIFF_COLUMN, LOAD_LDTS_COLUMN}
        else:
            exclude = {HASH_DIFF_COLUMN, LOAD_LDTS_COLUMN} | set(primary_key)

        columns_to_hash = [
            f.name for f in data.schema.fields if f.name not in exclude and not isinstance(f.dataType, (ArrayType, MapType, StructType))
        ]

        def _col_as_str(col_name: str):
            return F.coalesce(F.col(col_name).cast('string'), F.lit(''))

        coalesced_cols = [_col_as_str(c) for c in columns_to_hash]
        hash_input = F.concat_ws('', *coalesced_cols)

        data = data.withColumn(HASH_DIFF_COLUMN, F.sha2(hash_input, 256))
        data = data.withColumn(LOAD_LDTS_COLUMN, F.lit(now).cast('timestamp'))
        return data

    # ------------------------------------------------------------------
    # read  (mirrors DeltaClient.read)
    # ------------------------------------------------------------------

    def read(self, table_name: str) -> pyspark.sql.DataFrame:
        """Reads a Delta table and returns a Spark DataFrame."""
        uri = self._get_uri(table_name)
        logger.info(f'Reading Delta table from {uri}')
        return self.spark.read.format('delta').load(uri)

    # ------------------------------------------------------------------
    # write  (mirrors DeltaClient.write – idempotent upsert via hash-diff)
    # ------------------------------------------------------------------

    @retry(
        exceptions=Py4JJavaError,
        tries=SPARK_CLIENT_WRITE_RETRIES,
        delay=SPARK_CLIENT_WRITE_RETRY_DELAY,
        backoff=SPARK_CLIENT_WRITE_RETRY_BACKOFF,
        max_delay=SPARK_CLIENT_WRITE_RETRY_MAX_DELAY,
        jitter=SPARK_CLIENT_WRITE_RETRY_JITTER,
    )
    def write(
        self,
        table_name: str,
        data: pyspark.sql.DataFrame,
        primary_key: Union[str, Iterable] = None,
        partition_key: Optional[str] = None,
        add_metadata_columns: bool = True,
    ):
        """
        Main entry point. Performs an idempotent upsert using hash-diffing.
        Mirrors DeltaClient.write: computes hash_diff + ldts, creates the
        table on first run, then merges with a hash-diff guard so unchanged
        rows are never rewritten.
        """
        from delta.tables import DeltaTable

        if add_metadata_columns:
            data = self.__prepare_data(data, primary_key=primary_key, partition_key=partition_key)

        uri = self._get_uri(table_name)

        if not DeltaTable.isDeltaTable(self.spark, uri):
            writer = data.write.format('delta').mode('overwrite')
            if partition_key:
                writer = writer.partitionBy(partition_key)
            writer.option('overwriteSchema', 'true').save(uri)
            logger.info(f'Initialized new table {table_name}')
            return

        dt = DeltaTable.forPath(self.spark, uri)

        if isinstance(primary_key, str):
            merge_predicate = f'target.{primary_key} = source.{primary_key}'
        else:
            merge_predicate = ' AND '.join(f'target.{k} = source.{k}' for k in primary_key)

        if partition_key:
            merge_predicate += f' AND target.{partition_key} = source.{partition_key}'

        (
            dt.alias('target')
            .merge(data.alias('source'), merge_predicate)
            .whenMatchedUpdateAll(condition=f'target.{HASH_DIFF_COLUMN} != source.{HASH_DIFF_COLUMN}')
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f'Upserted batch into {table_name}.')

    # ------------------------------------------------------------------
    # get_count  (mirrors DeltaClient.get_count)
    # ------------------------------------------------------------------

    def get_count(self, table_name: str) -> int:
        """Returns the total number of rows in the Delta table."""
        uri = self._get_uri(table_name)
        return self.spark.read.format('delta').load(uri).count()

    # ------------------------------------------------------------------
    # Utilities (SparkClient-specific)
    # ------------------------------------------------------------------

    @staticmethod
    def assert_schema_matches(data: pyspark.sql.DataFrame, pa_schema: pa.Schema) -> None:
        """
        Validates that a Spark DataFrame contains the columns defined
        in a PyArrow schema (derived from a Pydantic model).
        """
        expected = {field.name for field in pa_schema}
        actual = set(data.columns)
        missing = sorted(expected - actual)
        if missing:
            raise ValueError(f'Missing columns for integrated schema contract: {missing}')
        extra = sorted(actual - expected)
        if extra:
            raise ValueError(f'Extra columns not in model contract (will be kept): {extra}')

    def stop(self):
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            logger.info('SparkSession stopped')


if __name__ == '__main__':
    client = SparkClient()
    print(client.spark)
    client.stop()
