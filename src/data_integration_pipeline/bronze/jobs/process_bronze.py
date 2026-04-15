from typing import Iterable, Optional

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.bronze.core.bronze_processor import BronzeProcessor
from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata, ChunkProcessingMetadata
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import ARCHIVED_BRONZE_DATA_FOLDER, METADATA_FILE_PATTERN


class ProcessBronzeJob:
    """
    Reads each chunk and writes a processed parquet file.
    """

    def __init__(self, data_source: Optional[str] = None):
        self.storage_client = CloudStorageClient()
        self.data_source = data_source

    def process_chunk(self, chunk_s3_path: str, parent_metadata_s3_path: str) -> ChunkProcessingMetadata:
        parent_metadata = BronzeToSilverProcessingMetadata(**self.storage_client.read_json(parent_metadata_s3_path))
        chunk_metadata = ChunkProcessingMetadata.create(
            parent_metadata_s3_path=parent_metadata_s3_path,
            data_source=parent_metadata.data_source,
            input_chunk_s3_path=chunk_s3_path,
        )
        processor = BronzeProcessor(parent_metadata=parent_metadata, chunk_metadata=chunk_metadata)
        # consumes the chunk and writes the processed data to the output processed s3 path
        # saves the chunk metadata and deletes the raw chunk
        return processor.run()

    def get_data_to_process(self) -> Iterable[dict]:
        # iterates over each parent metadata file
        for parent_metadata_s3_path in self.storage_client.get_files(prefix=ARCHIVED_BRONZE_DATA_FOLDER, regex_pattern=METADATA_FILE_PATTERN):
            # reads the parent metadata
            try:
                parent_metadata = BronzeToSilverProcessingMetadata(**self.storage_client.read_json(parent_metadata_s3_path))
            except Exception as e:
                logger.warning(f'Failed to read parent metadata {parent_metadata_s3_path}: {e}')
                continue
            # if the parent metadata is complete, skip
            if parent_metadata.end_timestamp:
                continue
            if self.data_source and parent_metadata.data_source != self.data_source:
                continue
            # iterates over each chunk in the parent metadata
            for chunk_s3_path in parent_metadata.raw_chunks:
                # if the raw chunk does not exist, it means its already processed
                if self.storage_client.file_exists(chunk_s3_path):
                    yield {
                        'chunk_s3_path': chunk_s3_path,
                        'parent_metadata_s3_path': parent_metadata_s3_path,
                    }

    def run(self):
        for task_data in self.get_data_to_process():
            chunk_metadata = self.process_chunk(**task_data)
            print(chunk_metadata)


def process_task(task_dict: dict) -> dict:
    job = ProcessBronzeJob()
    metadata = job.process_chunk(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = ProcessBronzeJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = ProcessBronzeJob()
    job.run()
