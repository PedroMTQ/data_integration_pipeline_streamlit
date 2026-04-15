from collections import defaultdict
from typing import Iterable, Optional

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.bronze.core.bronze_loader import BronzeLoader
from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata, ChunkProcessingMetadata
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import CHUNKED_BRONZE_DATA_FOLDER, METADATA_FILE_PATTERN


class LoadBronzeJob:
    """
    Phase 2: discovers processed parquet chunks and delegates Delta writes to SilverWriter.
    """

    def __init__(self, data_source: Optional[str] = None):
        self.storage_client = CloudStorageClient()
        self.data_source = data_source

    def process_data(self, parent_metadata_s3_path: str, chunk_metadata_s3_paths: list[str]) -> BronzeToSilverProcessingMetadata:
        parent_metadata = BronzeToSilverProcessingMetadata(**self.storage_client.read_json(parent_metadata_s3_path))
        chunks_metadata = [
            ChunkProcessingMetadata(**self.storage_client.read_json(chunk_metadata_s3_path)) for chunk_metadata_s3_path in chunk_metadata_s3_paths
        ]
        loader = BronzeLoader(parent_metadata=parent_metadata, chunks_metadata=chunks_metadata)
        loader.run()
        return parent_metadata

    def get_data_to_process(self) -> Iterable[dict]:
        chunks_to_parent = defaultdict(list)
        # iterates over each chunk metadata file, note that we only get a chunk metadata file if the chunk is processed
        for chunk_metadata_s3_path in self.storage_client.get_files(prefix=CHUNKED_BRONZE_DATA_FOLDER, regex_pattern=METADATA_FILE_PATTERN):
            metadata = ChunkProcessingMetadata(**self.storage_client.read_json(chunk_metadata_s3_path))
            raw_chunk_exists = self.storage_client.file_exists(metadata.input_chunk_s3_path)
            processed_chunk_exists = self.storage_client.file_exists(metadata.output_processed_s3_path)
            # if the raw chunk and processed chunk do not exist, it means there is a processing error
            if not raw_chunk_exists and not processed_chunk_exists:
                logger.warning(f'Chunk {metadata.input_chunk_s3_path} not found, you likely have a processing error!')
                continue
            chunks_to_parent[metadata.parent_metadata_s3_path].append(chunk_metadata_s3_path)
        already_yielded = set()
        for parent_metadata_s3_path, chunk_metadata_s3_paths in chunks_to_parent.items():
            try:
                parent_metadata = BronzeToSilverProcessingMetadata(**self.storage_client.read_json(parent_metadata_s3_path))
            except Exception as e:
                logger.warning(f'Failed to read parent metadata {parent_metadata_s3_path}: {e}')
                continue
            if parent_metadata.end_timestamp:
                for p in chunk_metadata_s3_paths:
                    self.storage_client.delete_file(p)
                continue
            if parent_metadata.output_delta_table in already_yielded:
                logger.debug(f'Already loading {parent_metadata.output_delta_table} in another task, skipping...')
                continue
            if self.data_source and parent_metadata.data_source != self.data_source:
                continue
            already_yielded.add(parent_metadata.output_delta_table)
            yield {
                'parent_metadata_s3_path': parent_metadata_s3_path,
                'chunk_metadata_s3_paths': chunk_metadata_s3_paths,
            }

    def run(self):
        for task_data in self.get_data_to_process():
            self.process_data(**task_data)


def process_task(task_dict: dict) -> dict:
    job = LoadBronzeJob()
    metadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = LoadBronzeJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = LoadBronzeJob()
    job.run()
