from typing import Optional

import pyarrow as pa
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.settings import DATA_BUCKET
from data_integration_pipeline.common.io.cloud_storage.s3_storage import S3FileWriter


class CloudFileWriter:
    def __init__(
        self,
        s3_path: str = DATA_BUCKET,
        bucket_name: Optional[str] = DATA_BUCKET,
        rows_chunk_size: int = 1000,
    ):
        logger.debug(f'Initializing CloudFileWriter for {s3_path}')
        self.writer = S3FileWriter(s3_path=s3_path, bucket_name=bucket_name, rows_chunk_size=rows_chunk_size)

    def write_table(self, table: pa.Table) -> None:
        self.writer.write_table(table)

    def write_row(self, row: dict) -> None:
        self.writer.write_row(row)

    def flush(self) -> None:
        self.writer.flush()

    def close(self) -> None:
        self.writer.close()

    def __enter__(self) -> 'CloudFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        return self.writer.__exit__(exc_type, exc_val, exc_tb)


if __name__ == '__main__':
    target_path = 'bronze/test.parquet'

    with CloudFileWriter(target_path, rows_chunk_size=2000) as writer:
        writer.write_row({'test': 1})

    with CloudFileWriter(target_path) as writer:
        # Small single row
        writer.write_row({'id': 1, 'data': 'start'})

        # Large bulk table from another process/reader
        my_table = pa.Table.from_pylist([{'id': i, 'data': 'bulk'} for i in range(2, 100)])
        writer.write_table(my_table)
