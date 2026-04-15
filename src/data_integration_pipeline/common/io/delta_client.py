import os
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional, Union

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from data_integration_pipeline.common.io.logger import logger
from deltalake import DeltaTable
from deltalake._internal import CommitFailedError
from deltalake.writer import write_deltalake
from retry import retry

from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.settings import (
    DATA_BUCKET,
    DELTA_CLIENT_BATCH_SIZE,
    DELTA_CLIENT_WRITE_RETRIES,
    DELTA_CLIENT_WRITE_RETRY_BACKOFF,
    DELTA_CLIENT_WRITE_RETRY_DELAY,
    DELTA_CLIENT_WRITE_RETRY_JITTER,
    DELTA_CLIENT_WRITE_RETRY_MAX_DELAY,
    HASH_DIFF_COLUMN,
    LOAD_LDTS_COLUMN,
    STORAGE_OPTIONS,
    UNKNOWN_PARTITION_STR,
)
from data_integration_pipeline.common.core.utils import get_timestamp


class DeltaClient:
    """
    Client to write pa.Tables into delta parquet tables.
    """

    def __init__(
        self,
        bucket_name: Optional[str] = DATA_BUCKET,
        storage_options: dict = None,
        batch_size: int = DELTA_CLIENT_BATCH_SIZE,
    ):
        """
        Initializes the client.
        base_path: e.g. 's3a://my-bucket' or 'abfss://container@account.dfs.core.windows.net'
        """
        if not bucket_name:
            raise ValueError('bucket_name or DATA_BUCKET must be set')
        self.batch_size = batch_size
        self.base_path = f's3a://{bucket_name}'
        self.storage_options = storage_options if storage_options else STORAGE_OPTIONS

    def _get_uri(self, table_name: str) -> str:
        return os.path.join(self.base_path, table_name)

    def get_current_version(self, table_name: str) -> int:
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, storage_options=self.storage_options)
            return dt.version()
        except Exception as e:
            print(f'Error reading changes for {table_name} due to {e}')
            return -1

    def get_last_commit_timestamp(self, table_name: str) -> Optional[datetime]:
        """Return the UTC datetime of the latest commit, or None if the table doesn't exist."""
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, storage_options=self.storage_options)
            history = dt.history(limit=1)
            if history:
                ts = history[0].get('timestamp')
                if ts is not None:
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        except Exception:
            return None
        return None

    def get_data_history(self, table_name: str):
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, storage_options=self.storage_options)
            table = dt.load_cdf(starting_version=1, ending_version=dt.version()).read_all()
            pt = pl.from_arrow(table)
            print(pt.sort('_commit_version', descending=True))
        except Exception as e:
            print(f'Error reading changes for {table_name} due to {e}')
            raise

    @staticmethod
    def __prepare_data(data: pa.Table, primary_key: Union[str, Iterable], partition_key: str) -> pa.Table:
        if not primary_key:
            raise Exception('Missing primary_key')
        now = get_timestamp()
        original_schema = data.schema
        df = pl.from_arrow(data)
        if partition_key:
            df = df.with_columns(pl.col(partition_key).fill_null(UNKNOWN_PARTITION_STR))
        if isinstance(primary_key, str):
            exclude = {primary_key, HASH_DIFF_COLUMN, LOAD_LDTS_COLUMN}
        elif isinstance(primary_key, Iterable):
            exclude = {HASH_DIFF_COLUMN, LOAD_LDTS_COLUMN} | set(primary_key)
        columns_to_hash = [name for name, dtype in df.schema.items() if name not in exclude and not dtype.is_nested()]
        df = df.with_columns(
            [
                # sets hash diff
                pl.concat_str(pl.col(columns_to_hash), ignore_nulls=True).hash().cast(pl.String).alias(HASH_DIFF_COLUMN),
                # sets load timestamp
                pl.lit(now).alias(LOAD_LDTS_COLUMN),
            ]
        )
        result = df.to_arrow()
        target_schema = original_schema.append(pa.field(HASH_DIFF_COLUMN, result.schema.field(HASH_DIFF_COLUMN).type, nullable=False))
        data = result.cast(target_schema)
        return data

    @retry(
        exceptions=CommitFailedError,
        tries=DELTA_CLIENT_WRITE_RETRIES,
        delay=DELTA_CLIENT_WRITE_RETRY_DELAY,
        backoff=DELTA_CLIENT_WRITE_RETRY_BACKOFF,
        max_delay=DELTA_CLIENT_WRITE_RETRY_MAX_DELAY,
        jitter=DELTA_CLIENT_WRITE_RETRY_JITTER,
    )
    def write(
        self, table_name: str, data: pa.Table, primary_key: Union[str, Iterable] = None, partition_key: str = None, add_metadata_columns: bool = True
    ) -> None:
        """
        Main entry point. Performs an idempotent upsert using hash-diffing.
        """
        uri = self._get_uri(table_name)
        if add_metadata_columns:
            data = self.__prepare_data(data=data, primary_key=primary_key, partition_key=partition_key)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            write_deltalake(
                uri,
                data=data,
                partition_by=[partition_key] if partition_key else None,
                storage_options=self.storage_options,
            )
            logger.info(f'Initialized new table {table_name} with {len(data)} records.')
            return
        dt = DeltaTable(uri, storage_options=self.storage_options)
        mapping = {col: f'source.{col}' for col in data.schema.names if col != primary_key}
        if isinstance(primary_key, str):
            merge_predicate = f'target.{primary_key} = source.{primary_key}'
        elif isinstance(primary_key, Iterable):
            merge_predicate = []
            for k in primary_key:
                merge_predicate.append(f'target.{k} = source.{k}')
            merge_predicate = ' AND '.join(merge_predicate)

        if partition_key:
            merge_predicate += f' AND target.{partition_key} = source.{partition_key}'
        (
            dt.merge(source=data, predicate=merge_predicate, source_alias='source', target_alias='target')
            .when_matched_update(
                updates=mapping,
                # This ensures we don't write a new version if the data is identical
                predicate=f'target.{HASH_DIFF_COLUMN} != source.{HASH_DIFF_COLUMN}',
            )
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info(f'Upserted batch into {table_name}.')

    def _build_projection(self, table_name: str, dt: DeltaTable, columns: list = None) -> dict:
        partition_key = ModelMapper.get_partition_key(table_name)
        all_column_names = [f.name for f in dt.schema().fields]
        projection_cols = columns if columns else all_column_names
        projection = {col: ds.field(col) for col in projection_cols}
        # null partition values are replaced with UNKNOWN_PARTITION_STR at write time, so we revert them back to null here
        if partition_key and partition_key in projection:
            projection[partition_key] = pc.if_else(
                pc.equal(ds.field(partition_key), UNKNOWN_PARTITION_STR), pa.scalar(None, type=pa.string()), ds.field(partition_key)
            )
        return projection

    def read(
        self,
        table_name: str,
        columns: list = None,
        version: Union[int, datetime] = None,
        filter_expr: Optional[ds.Expression] = None,
    ) -> Iterator[pa.RecordBatch]:
        """
        Direct Arrow streaming from Delta Lake.
        Zero Polars, Zero memory overhead.
        Use filter_expr for generic PyArrow Dataset filtering.
        """
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, version=version, storage_options=self.storage_options)
        projection = self._build_projection(table_name=table_name, dt=dt, columns=columns)
        return dt.to_pyarrow_dataset().to_batches(columns=projection, filter=filter_expr, batch_size=self.batch_size)

    def read_fragments(
        self,
        table_name: str,
        columns: list = None,
        version: Union[int, datetime] = None,
        min_fragment_index: Optional[int] = None,
    ) -> Iterator[tuple[int, Iterator[pa.RecordBatch]]]:
        """Yield ``(fragment_index, batch_iterator)`` for each Parquet file in the table."""
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, version=version, storage_options=self.storage_options)
        projection = self._build_projection(table_name=table_name, dt=dt, columns=columns)
        dataset = dt.to_pyarrow_dataset()
        for i, fragment in enumerate(dataset.get_fragments()):
            if min_fragment_index is not None and i < min_fragment_index:
                continue
            yield i, fragment.to_batches(columns=projection, batch_size=self.batch_size, schema=dataset.schema)

    def rollback(self, table_name: str, version: int = None, timestamp: datetime = None):
        """Restores table to a previous state using Delta Time Travel."""
        if version is None and timestamp is None:
            raise Exception('Missing version and timestamp')
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, storage_options=self.storage_options)
        if version is not None:
            dt.restore(version)
        elif timestamp is not None:
            dt.restore(timestamp)
        logger.info(f'Rolled back DeltaTable {table_name} to {version or timestamp}')

    def optimize_table(
        self,
        table_name: str,
        partition_filters: Any | None = None,
        target_size: int | None = None,
        max_concurrent_tasks: int | None = None,
        min_commit_interval: int | timedelta | None = None,
        z_order_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Optimizes a Delta table via compaction or z-ordering and returns metrics."""
        uri = self._get_uri(table_name)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            logger.warning(f'Skipping optimize: {table_name} is not a Delta table')
            return {}
        dt = DeltaTable(uri, storage_options=self.storage_options)
        if z_order_columns:
            metrics = dt.optimize.z_order(
                columns=z_order_columns,
                partition_filters=partition_filters,
                target_size=target_size,
                max_concurrent_tasks=max_concurrent_tasks,
                min_commit_interval=min_commit_interval,
            )
            logger.info(f'Z-ordered Delta table {table_name} using columns={z_order_columns}: {metrics}')
            return metrics
        metrics = dt.optimize.compact(
            partition_filters=partition_filters,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            min_commit_interval=min_commit_interval,
        )
        return metrics

    def vacuum_table(
        self, table_name: str, retention_hours: int | None = None, dry_run: bool = True, enforce_retention_duration: bool = True
    ) -> list[str]:
        """Runs vacuum on a Delta table and returns file paths selected for deletion."""
        uri = self._get_uri(table_name)
        if not DeltaTable.is_deltatable(uri, storage_options=self.storage_options):
            logger.warning(f'Skipping vacuum: {table_name} is not a Delta table')
            return []
        dt = DeltaTable(uri, storage_options=self.storage_options)
        removed_files = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
        )
        logger.info(f'Vacuumed Delta table {table_name}: {len(removed_files)} files')
        return removed_files

    def get_count(self, table_name: str, version: Union[int, datetime] = None) -> int:
        """Returns the total number of rows in the Delta table using transaction log stats (no data scan)."""
        uri = self._get_uri(table_name)
        try:
            dt = DeltaTable(uri, version=version, storage_options=self.storage_options)
            return dt.count()
        except Exception as e:
            logger.error(f'Error getting count for {table_name}: {e}')
            raise

    def get_schema(self, table_name: str) -> pa.Schema:
        uri = self._get_uri(table_name)
        dt = DeltaTable(uri, storage_options=self.storage_options)
        return dt.schema().to_arrow()


if __name__ == '__main__':
    client = DeltaClient()
    table_path = 'silver/dataset_3/records.delta'
    print(client.get_count(table_path))
    schema = client.get_schema(table_path)
    print(schema)
    for batch in client.read(table_path):
        table = pl.from_arrow(batch)
        print(table)
        break
    # for field in schema:
    #     print(field.name, field.type, field.nullable)
    # print(repr(client.get_last_commit_timestamp(table_path)))
    # reader = client.read_fragments(
    #     table_name=table_path,
    # )
    # for fragment_index, batches in reader:
    #     print(fragment_index)
    #     for batch in batches:
    #         table = pl.from_arrow(batch)
    #         print(table)
    #         break
    #     break
    # print(data)
    # print(client.get_count(table_path))
    # print(client.get_current_version(table_path))
