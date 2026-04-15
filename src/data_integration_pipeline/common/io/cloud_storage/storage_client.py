import re
from typing import Optional, Union

from data_integration_pipeline.common.io.logger import logger
from pydantic import BaseModel

from data_integration_pipeline.settings import DATA_BUCKET
from data_integration_pipeline.common.io.cloud_storage.s3_storage.s3_client import S3Client


class CloudStorageClient:
    """Thin wrapper that delegates to S3Client."""

    def __init__(
        self,
        bucket_name: str = DATA_BUCKET,
        **kwargs,
    ):
        logger.debug(f'Initializing CloudStorageClient for bucket {bucket_name}')
        self._client = S3Client(bucket_name=bucket_name, **kwargs)

    def test_connection(self):
        return self._client.test_connection()

    def get_files(self, prefix: str = None, regex_pattern: Optional[re.Pattern] = None) -> list[str]:
        return self._client.get_files(prefix=prefix, regex_pattern=regex_pattern)

    def get_delta_tables(self, prefix: str = 'silver') -> list[str]:
        return self._client.get_delta_tables(prefix=prefix)

    def delete_file(self, s3_path: str) -> bool:
        return self._client.delete_file(s3_path=s3_path)

    def move_file(self, current_path: str, new_path: str, current_bucket: Optional[str] = None, new_bucket: Optional[str] = None) -> str:
        return self._client.move_file(current_path=current_path, new_path=new_path, current_bucket=current_bucket, new_bucket=new_bucket)

    def copy_file(self, current_path: str, new_path: str, current_bucket: Optional[str] = None, new_bucket: Optional[str] = None) -> str:
        return self._client.copy_file(current_path=current_path, new_path=new_path, current_bucket=current_bucket, new_bucket=new_bucket)

    def download_file(self, s3_path: str, output_folder: str) -> str:
        return self._client.download_file(s3_path=s3_path, output_folder=output_folder)

    def upload_file(self, local_path: str, s3_path: str) -> bool:
        return self._client.upload_file(local_path=local_path, s3_path=s3_path)

    def read_json(self, s3_path: str) -> dict:
        return self._client.read_json(s3_path)

    def write_json(self, s3_path: str, data: Union[dict, str, BaseModel]) -> None:
        return self._client.write_json(s3_path, data)

    def file_exists(self, s3_path: str) -> bool:
        return self._client.file_exists(s3_path=s3_path)
