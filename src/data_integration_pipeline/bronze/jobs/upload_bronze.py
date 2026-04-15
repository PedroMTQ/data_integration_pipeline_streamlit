import os

from data_integration_pipeline.common.io.logger import logger
from typing import Optional

from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import BRONZE_DATA_FOLDER, TESTS_DATA


class UploadBronzeJob:
    def __init__(self, data_source: Optional[str] = None):
        self.storage_client = CloudStorageClient()
        self.data_source = data_source

    def run(self):
        bronze_data_folder = os.path.join(TESTS_DATA, BRONZE_DATA_FOLDER)
        for file in os.listdir(bronze_data_folder):
            data_source_folder = os.path.join(bronze_data_folder, file)
            if not os.path.isdir(data_source_folder):
                continue
            data_model = ModelMapper.get_data_model(data_source_folder)
            if not data_model:
                continue
            if self.data_source and data_model.data_source != self.data_source:
                continue
            data_source = data_model.data_source
            for data_file in os.listdir(data_source_folder):
                local_file_path = os.path.join(data_source_folder, data_file)
                s3_path = os.path.join(BRONZE_DATA_FOLDER, data_source, data_file)
                logger.info(f'Uploading {local_file_path} to {s3_path}')
                upload_status = self.storage_client.upload_file(local_path=local_file_path, s3_path=s3_path)
                if upload_status:
                    logger.info(f'Uploaded {local_file_path} to {s3_path}')
                else:
                    logger.warning(f'Failed to upload {local_file_path} to {s3_path}')


if __name__ == '__main__':
    job = UploadBronzeJob()
    job.run()
