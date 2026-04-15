"""
Downloads the output data from the S3 bucket to the tmp folder
unzips the file
Then loads the data into data/silver/integrated/records.delta
it also runs sync-pg and sync-es job
"""

import os
import zipfile
from pathlib import Path

import requests

from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.settings import TEMP

FILE_PREFIX = 'https://pmtq-data.fsn1.your-objectstorage.com/'
STREAM_CHUNK_SIZE = 64 * 1024
ZIP_FILE = 'dip_test_data/output/integrated_delta.zip'
OUTPUT_DELTA_TABLE = 'silver/integrated/records.delta'


class LoadOutputDataJob:
    def _download_zip(self, zip_path: str) -> None:
        if os.path.isfile(zip_path):
            logger.info(f'Zip already exists at {zip_path}, skipping download.')
            return
        cloud_url = f'{FILE_PREFIX}{ZIP_FILE}'
        Path(zip_path).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f'Downloading {cloud_url} -> {zip_path}')
        with requests.get(cloud_url, stream=True) as response:
            response.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
        logger.info('Download finished.')

    def _extract_zip(self, zip_path: str, extract_dir: str) -> None:
        logger.info(f'Extracting {zip_path} -> {extract_dir}')
        Path(extract_dir).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_dir)
        logger.info(f'Extraction complete: {os.listdir(extract_dir)}')

    @staticmethod
    def _find_delta_root(extract_dir: str) -> str:
        """Walk the extracted directory to find the folder that contains _delta_log."""
        for root, dirs, _files in os.walk(extract_dir):
            if '_delta_log' in dirs:
                return root
        raise FileNotFoundError(f'No _delta_log directory found under {extract_dir}')

    def _upload_delta_table(self, local_delta_dir: str) -> None:
        delta_root = self._find_delta_root(local_delta_dir)
        logger.info(f'Found delta table root at {delta_root}')
        storage_client = CloudStorageClient()
        for root, _dirs, files in os.walk(delta_root):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                relative = os.path.relpath(local_path, delta_root)
                s3_path = f'{OUTPUT_DELTA_TABLE}/{relative}'
                storage_client.upload_file(local_path=local_path, s3_path=s3_path)
        logger.info(f'Delta table uploaded to {OUTPUT_DELTA_TABLE}')

    @staticmethod
    def _run_sync_pg() -> None:
        from data_integration_pipeline.gold.jobs.sync_postgres import SyncPostgresJob

        logger.info('Running sync-pg...')
        SyncPostgresJob().run()

    @staticmethod
    def _run_pg_report() -> None:
        from data_integration_pipeline.gold.jobs.create_pg_report import CreatePostgresReportJob

        logger.info('Running pg-report...')
        CreatePostgresReportJob().run()

    @staticmethod
    def _run_sync_es() -> None:
        from data_integration_pipeline.gold.jobs.sync_elastic_search import SyncOrganizationsElasticsearchJob

        logger.info('Running sync-es...')
        SyncOrganizationsElasticsearchJob().run()

    def run(self) -> None:
        zip_path = os.path.join(TEMP, 'integrated_delta.zip')
        extract_dir = os.path.join(TEMP, 'integrated_delta')

        self._download_zip(zip_path)
        self._extract_zip(zip_path, extract_dir)
        self._upload_delta_table(extract_dir)
        self._run_sync_pg()
        self._run_pg_report()
        self._run_sync_es()

        logger.info('LoadOutputDataJob complete.')


if __name__ == '__main__':
    LoadOutputDataJob().run()
