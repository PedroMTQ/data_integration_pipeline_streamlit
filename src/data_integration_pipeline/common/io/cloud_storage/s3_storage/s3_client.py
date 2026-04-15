import s3fs
import json
import os
import re
from pathlib import Path
from typing import Optional, Union

from data_integration_pipeline.common.io.logger import logger
from pydantic import BaseModel

from data_integration_pipeline.settings import DELTA_TABLE_SUFFIX


from data_integration_pipeline.common.io.cloud_storage.s3_storage.base_client import get_s3_client


class S3Client:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.fs: s3fs.S3FileSystem = get_s3_client()
        self.test_connection()

    def _full_path(self, key: str, bucket_name: Optional[str] = None) -> str:
        return f'{bucket_name or self.bucket_name}/{key.lstrip("/")}'

    def test_connection(self) -> bool:
        try:
            self.fs.ls(self.bucket_name)
            logger.debug(f'Connected to {self.bucket_name}')
            return True
        except Exception as e:
            logger.error(f'Failed to connect to {self.bucket_name}')
            raise Exception(f'Error accessing {self.bucket_name} due to: {e}') from e

    def get_files(self, prefix: str = None, regex_pattern: re.Pattern = None) -> list[str]:
        if regex_pattern is not None and not isinstance(regex_pattern, re.Pattern):
            raise TypeError(f'Invalid type for regex_pattern: {type(regex_pattern)}, should be re.Pattern')
        try:
            full_prefix = self._full_path(prefix) if prefix else self.bucket_name
            raw_paths = self.fs.find(full_prefix)
            strip = f'{self.bucket_name}/'
            res: list[str] = []
            for p in raw_paths:
                key = p[len(strip) :] if p.startswith(strip) else p
                if regex_pattern is not None:
                    if regex_pattern.search(key):
                        res.append(key)
                    else:
                        logger.debug(f'File skipped: {key}')
                else:
                    res.append(key)
        except Exception as e:
            raise RuntimeError(f'Error listing files under {self.bucket_name}/{prefix} due to: {e}') from e
        logger.debug(f'Files returned from {prefix}: {res}')
        return res

    def get_delta_tables(self, prefix: str) -> list[str]:
        delta_roots: set[str] = set()
        try:
            for file_path in self.get_files(prefix=prefix):
                if DELTA_TABLE_SUFFIX in file_path:
                    parts = file_path.split('/')
                    for i, part in enumerate(parts):
                        if part.endswith(DELTA_TABLE_SUFFIX):
                            root_path = '/'.join(parts[: i + 1])
                            delta_roots.add(root_path)
                            break
        except Exception as e:
            logger.error(f'Error listing Delta tables: {e}')
            raise
        res = sorted(delta_roots)
        logger.debug(f'Delta tables found under {prefix}: {res}')
        return res

    def delete_file(self, s3_path: str) -> bool:
        try:
            self.fs.rm(self._full_path(s3_path))
            return True
        except Exception as e:
            logger.exception(f'Failed to delete {s3_path} due to {e}')
            return False

    def move_file(self, current_path: str, new_path: str, current_bucket: Optional[str] = None, new_bucket: Optional[str] = None) -> str:
        full_current_path = self._full_path(key=current_path, bucket_name=current_bucket or self.bucket_name)
        full_new_path = self._full_path(key=new_path, bucket_name=new_bucket or self.bucket_name)
        try:
            self.fs.move(full_current_path, full_new_path)
        except Exception as e:
            logger.exception(f'Failed to move {full_current_path} to {full_new_path} due to {e}')
            return None
        logger.info(f'Moved file from {full_current_path} to {full_new_path}')
        return new_path

    def copy_file(self, current_path: str, new_path: str, current_bucket: Optional[str] = None, new_bucket: Optional[str] = None) -> str:
        full_current_path = self._full_path(key=current_path, bucket_name=current_bucket or self.bucket_name)
        full_new_path = self._full_path(key=new_path, bucket_name=new_bucket or self.bucket_name)
        try:
            self.fs.copy(full_current_path, full_new_path)
        except Exception as e:
            logger.exception(f'Failed to move {full_current_path} to {full_new_path} due to {e}')
            return None
        logger.info(f'Moved file from {full_current_path} to {full_new_path}')
        return new_path

    def download_file(self, s3_path: str, output_folder: str) -> str:
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        file_name = Path(s3_path).name
        local_path = os.path.join(output_folder, file_name)
        self.fs.get(self._full_path(s3_path), local_path)
        logger.debug(f'Downloaded {s3_path} to {output_folder}')
        return local_path

    def upload_file(self, local_path: str, s3_path: str) -> bool:
        try:
            self.fs.put(local_path, self._full_path(s3_path))
            return True
        except Exception as e:
            logger.error(f'Failed to upload {local_path} to {s3_path} due to {e}')
            return False

    def __read_file(self, s3_path: str) -> bytes:
        return self.fs.cat(self._full_path(s3_path))

    def read_json(self, s3_path: str) -> dict:
        return json.loads(self.__read_file(s3_path))

    def write_json(self, s3_path: str, data: Union[dict, str, BaseModel]) -> None:
        if hasattr(data, 'model_dump_json'):
            json_str = data.model_dump_json()
        elif isinstance(data, dict):
            json_str = json.dumps(data, default=str)
        elif isinstance(data, str):
            json_str = data
        else:
            raise TypeError(f'Invalid data type <{type(data)}> for {data}')
        json_bytes = json_str.encode('utf-8')
        self.fs.pipe(self._full_path(s3_path), json_bytes)
        logger.debug(f'JSON data stored in {s3_path}')

    def file_exists(self, s3_path: str) -> bool:
        return self.fs.exists(self._full_path(s3_path))
