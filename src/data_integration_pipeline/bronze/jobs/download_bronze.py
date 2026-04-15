import os
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.settings import BRONZE_DATA_FOLDER, TESTS_DATA
import requests

from pathlib import Path

FILE_PREFIX = 'https://pmtq-data.fsn1.your-objectstorage.com/dip_test_data/tests/data/bronze/'
STREAM_CHUNK_SIZE = 64 * 1024
FILES_TO_DOWNLOAD = [
    'dataset_1/dataset_1_generated.json',
    'dataset_2/dataset_2_generated.json',
    'dataset_3/dataset_3.parquet',
]


class DownloadBronzeJob:
    def run(self) -> None:
        for file_path in FILES_TO_DOWNLOAD:
            cloud_file_path = f'{FILE_PREFIX}{file_path}'
            # /home/pedroq/workspace/data_integration_pipeline_comp/tests/data/bronze/dataset_1/dataset_1_generated.json
            local_file_path = os.path.join(TESTS_DATA, BRONZE_DATA_FOLDER, file_path)
            if os.path.isfile(local_file_path):
                continue
            Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)
            logger.info(f'Downloading {cloud_file_path} -> {local_file_path}')
            with requests.get(cloud_file_path, stream=True) as response:
                response.raise_for_status()
                with open(local_file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
        logger.info('Download finished.')


if __name__ == '__main__':
    DownloadBronzeJob().run()
