from typing import Iterable, Optional, Union

from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
import pyarrow.dataset as ds
import s3fs

from data_integration_pipeline.common.io.cloud_storage.s3_storage.base_client import get_s3_client

from pathlib import Path

import json_stream
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pa_csv

from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.settings import CSV_SUFFIX, JSONL_SUFFIX, JSON_SUFFIX, PARQUET_SUFFIX


class S3FileReader:
    def __init__(
        self,
        s3_path: str,
        bucket_name: str,
        as_table: bool = True,
        chunk_size: int = 10000,
        filter_expr: Optional[ds.Expression] = None,
        columns: Optional[list[str]] = None,
        schema: Optional[pa.Schema] = None,
    ):
        self.bucket_name = bucket_name
        self.as_table = as_table
        self.filter_expr = filter_expr
        self.columns = columns
        self.schema = schema
        self.chunk_size = int(chunk_size)
        self.extension = Path(s3_path).suffix.lower()
        self._file_handle = None
        self._generator = None
        self.fs: s3fs.S3FileSystem = get_s3_client()
        clean_path = s3_path.lstrip('/')
        self._file_path = f's3://{self.bucket_name}/{clean_path}' if not s3_path.startswith('s3://') else s3_path
        if not self.schema:
            self.schema = self._infer_schema()

    def _infer_schema(self) -> pa.Schema:
        data_model = ModelMapper.get_data_model(self._file_path)
        if not data_model:
            raise ValueError(f'No data model found for {self._file_path}')
        if data_model:
            return data_model._raw_schema
        if self.extension == CSV_SUFFIX:
            with self.fs.open(self._file_path, mode='rb') as peek_handle:
                first_chunk = pa_csv.read_csv(peek_handle)
                column_names = first_chunk.column_names
            return {name: pa.string() for name in column_names}
        elif self.extension == PARQUET_SUFFIX:
            return self.get_parquet_schema()
        elif self.extension == JSON_SUFFIX:
            with self.fs.open(self._file_path, mode='rb') as peek_handle:
                first_row = next(iter(json_stream.load(peek_handle)))
                first_row = json_stream.to_standard_types(first_row)
                return pa.Table.from_pylist([first_row]).schema
        else:
            raise ValueError(f'Unsupported extension: {self.extension}')

    def _read_json(self) -> Iterable[Union[list[dict], pa.Table]]:
        self._file_handle = self.fs.open(self._file_path, mode='rb')
        buffer: list[dict] = []

        def rows_to_table(rows: list[dict]) -> pa.Table:
            return pa.Table.from_pylist(rows, schema=self.schema)

        def emit(table: pa.Table) -> Iterable[Union[list[dict], pa.Table]]:
            table = self._apply_filter(table)
            table = self._apply_projection(table)
            if table.num_rows == 0:
                return
            for i in range(0, table.num_rows, self.chunk_size):
                yield from self._finalize_yield(table.slice(i, self.chunk_size))

        for raw in json_stream.load(self._file_handle):
            row = json_stream.to_standard_types(raw)
            if not isinstance(row, dict):
                continue
            buffer.append(row)
            while len(buffer) >= self.chunk_size:
                chunk = buffer[: self.chunk_size]
                del buffer[: self.chunk_size]
                yield from emit(rows_to_table(chunk))
        if buffer:
            yield from emit(rows_to_table(buffer))

    def _read_parquet(self) -> Iterable[Union[list[dict], pa.Table]]:
        self._file_handle = self.fs.open(self._file_path, mode='rb')
        parquet_file = pq.ParquetFile(self._file_handle)
        for batch in parquet_file.iter_batches(batch_size=self.chunk_size, columns=self.columns):
            table = pa.Table.from_batches([batch])
            table = self._apply_filter(table)
            table = self._apply_projection(table)
            if table.num_rows == 0:
                continue
            yield from self._finalize_yield(table)

    def _read_csv(self) -> Iterable[Union[list[dict], pa.Table]]:
        self._file_handle = self.fs.open(self._file_path, mode='rb')
        convert_options = pa_csv.ConvertOptions(column_types=self.schema)
        kwargs = {
            'read_options': pa_csv.ReadOptions(block_size=10 * 1024 * 1024),
        }
        if convert_options is not None:
            kwargs['convert_options'] = convert_options
        reader = pa_csv.open_csv(self._file_handle, **kwargs)
        for batch in reader:
            table = pa.Table.from_batches([batch])
            table = self._apply_filter(table)
            table = self._apply_projection(table)
            if table.num_rows == 0:
                continue
            for i in range(0, table.num_rows, self.chunk_size):
                yield from self._finalize_yield(table.slice(i, self.chunk_size))

    def get_parquet_row_count(self) -> int:
        if Path(self._file_path).suffix.lower() != PARQUET_SUFFIX:
            raise ValueError('get_parquet_row_count only supports parquet files')
        with self.fs.open(self._file_path, mode='rb') as f:
            parquet_file = pq.ParquetFile(f)
            return parquet_file.metadata.num_rows

    def get_parquet_schema(self) -> pa.Schema:
        """Read schema from parquet file metadata only (no row data read)."""
        if Path(self._file_path).suffix.lower() != PARQUET_SUFFIX:
            raise ValueError('get_parquet_schema only supports parquet files')
        with self.fs.open(self._file_path, mode='rb') as f:
            parquet_file = pq.ParquetFile(f)
            return parquet_file.schema_arrow

    def __iter__(self):
        if self._generator is not None:
            return self._generator
        if self.extension == CSV_SUFFIX:
            self._generator = self._read_csv()
        elif self.extension == PARQUET_SUFFIX:
            self._generator = self._read_parquet()
        elif self.extension in (JSON_SUFFIX, JSONL_SUFFIX):
            self._generator = self._read_json()
        else:
            raise ValueError(f'Unsupported extension: {self.extension}')

        return self._generator

    def __next__(self):
        return next(iter(self))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file_handle:
            logger.debug(f'Closing stream for {self._file_path}')
            self._file_handle.close()

    def _finalize_yield(self, table: pa.Table) -> Iterable[Union[list[dict], pa.Table]]:
        """Final output formatter based on as_table flag."""
        if self.as_table:
            yield table
        else:
            yield from table.to_pylist()

    def _apply_filter(self, table: pa.Table) -> pa.Table:
        if self.filter_expr is None:
            return table
        return ds.dataset(table).to_table(filter=self.filter_expr)

    def _apply_projection(self, table: pa.Table) -> pa.Table:
        if not self.columns:
            return table
        return table.select(self.columns)

    def read_table(self) -> pl.DataFrame:
        self.as_table = True
        return pl.from_arrow(pa.concat_tables(self))
