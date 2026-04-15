from typing import Iterable

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.auditor.core.audit_runner import AuditRunner
from data_integration_pipeline.auditor.core.metadata.audit_metadata import AuditMetadata
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import SILVER_DATA_FOLDER


class AuditSilverDataJob:
    def __init__(self):
        self.storage_client = CloudStorageClient()

    def process_data(self, s3_path: str) -> AuditMetadata:
        metadata = AuditMetadata.create(s3_path=s3_path, dataset_stage='silver')
        try:
            runner = AuditRunner(s3_path=s3_path, dataset_stage='silver')
            metadata: AuditMetadata = runner.run(metadata)
        except Exception as e:
            logger.exception(f'Audit failed for {s3_path}')
            metadata.error_message = str(e)
        finally:
            metadata.complete(metadata.metadata_s3_path)
        return metadata

    def get_data_to_process(self) -> Iterable[str]:
        for silver_s3_path in self.storage_client.get_delta_tables(prefix=SILVER_DATA_FOLDER):
            yield silver_s3_path

    def run(self):
        for s3_path in self.get_data_to_process():
            self.process_data(s3_path)


def process_task(s3_path: str) -> dict:
    job = AuditSilverDataJob()
    metadata: AuditMetadata = job.process_data(s3_path)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[str]:
    job = AuditSilverDataJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = AuditSilverDataJob()
    job.run()
