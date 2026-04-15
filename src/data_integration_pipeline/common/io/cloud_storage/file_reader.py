from typing import Optional

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.settings import CSV_SUFFIX, DATA_BUCKET, PARQUET_SUFFIX
from data_integration_pipeline.common.io.cloud_storage.s3_storage.file_reader import S3FileReader


class CloudFileReader:
    def __init__(
        self,
        s3_path: str,
        bucket_name: str = DATA_BUCKET,
        as_table: bool = True,
        chunk_size: int = 10000,
        filter_expr: Optional[ds.Expression] = None,
        columns: Optional[list[str]] = None,
        **kwargs,
    ):
        logger.debug(f'Initializing CloudFileReader for {s3_path} with s3 backend')
        self.reader = S3FileReader(
            s3_path=s3_path,
            bucket_name=bucket_name,
            as_table=as_table,
            chunk_size=chunk_size,
            filter_expr=filter_expr,
            columns=columns,
            **kwargs,
        )
        self.schema = self.reader.schema

    def get_row_count(self) -> int:
        if self.reader.extension == CSV_SUFFIX:
            return self.__get_csv_row_count()
        elif self.reader.extension == PARQUET_SUFFIX:
            return self.__get_parquet_row_count()
        else:
            raise ValueError(f'Unsupported extension: {self.reader.extension}')

    def __get_parquet_row_count(self) -> int:
        return self.reader.get_parquet_row_count()

    def __get_csv_row_count(self) -> int:
        """Count CSV rows by streaming batches (avoids building a full table in memory)."""
        total = 0
        for batch in self.reader:
            total += batch.num_rows if hasattr(batch, 'num_rows') else len(batch)
        return total

    def get_parquet_schema(self) -> pa.Schema:
        return self.reader.get_parquet_schema()

    def __iter__(self):
        return iter(self.reader)

    def __next__(self):
        return next(self.reader)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.reader.__exit__(exc_type, exc_val, exc_tb)

    def read_table(self) -> pl.DataFrame:
        return self.reader.read_table()


if __name__ == '__main__':
    from data_integration_pipeline.common.core.models.model_mapper import ModelMapper

    file_path = 'chunked_bronze/dataset_3/019d7d37-290a-77f0-a247-d64a4c93596d/chunk_1.parquet'
    data_model = ModelMapper.get_data_model(file_path)
    reader = CloudFileReader(s3_path=file_path)
    print(reader.schema)
    print('#' * 30)
    print(reader.get_parquet_schema())
