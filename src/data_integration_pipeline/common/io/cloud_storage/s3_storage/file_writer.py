import s3fs

from data_integration_pipeline.common.io.cloud_storage.s3_storage.base_client import get_s3_client
from pathlib import Path
from typing import Any, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.settings import PARQUET_SUFFIX


class S3FileWriter:
    def __init__(
        self,
        s3_path: str,
        bucket_name: str,
        rows_chunk_size: int = 1000,
    ):
        if not bucket_name:
            raise ValueError('bucket_name must be provided.')
        self.bucket_name = bucket_name
        self.rows_chunk_size = rows_chunk_size

        self.extension = Path(s3_path).suffix.lower()
        if self.extension not in [PARQUET_SUFFIX]:
            raise ValueError(f'Unsupported extension: <{self.extension}>')
        # Internal state
        self._buffer: list[dict[str, Any]] = []
        self._writer: Optional[pq.ParquetWriter] = None
        self._file_handle = None

        clean_path = s3_path.lstrip('/')
        self._file_path = f's3://{self.bucket_name}/{clean_path}' if not s3_path.startswith('s3://') else s3_path
        self.fs: s3fs.S3FileSystem = get_s3_client()

    def write_table(self, table: Union[pa.Table, pa.RecordBatch]):
        """Writes a Table or RecordBatch directly to the Parquet stream."""

        # RecordBatch to Table promotion
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])
        # If we have rows in the buffer, flush them first to maintain order
        if self._buffer:
            self.flush()

        self._stream_to_parquet(table)

    def write_row(self, row: dict[str, Any]):
        """Buffers a single dictionary and flushes when rows_chunk_size is hit."""

        self._buffer.append(row)
        if len(self._buffer) >= self.rows_chunk_size:
            self.flush()

    def flush(self):
        """Converts buffered dicts to a table and streams to Parquet."""
        if not self._buffer:
            return

        table = pa.Table.from_pylist(self._buffer)
        self._stream_to_parquet(table)
        self._buffer = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            # Final flush for row buffer
            self.flush()
        finally:
            self.close()

    def _stream_to_parquet(self, table: pa.Table):
        if self._writer is None:
            self._file_handle = self.fs.open(self._file_path, mode='wb')
            self._writer = pq.ParquetWriter(
                self._file_handle,
                table.schema,
                compression='snappy',
            )
        self._writer.write_table(table)

    def close(self):
        if self._writer:
            self._writer.close()
            self._writer = None
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None
        logger.debug(f'Closed writer for {self._file_path}')
