from typing import Iterable, Optional

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.bronze.core.bronze_chunker import Chunker
from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata
from data_integration_pipeline.common.io.cloud_storage import CloudFileReader, CloudStorageClient
from data_integration_pipeline.settings import BRONZE_DATA_FILE_PATTERN, BRONZE_DATA_FOLDER, DEFAULT_CHUNK_SIZE
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper


class ChunkBronzeJob:
    """Chunk raw bronze data into smaller parquet files."""

    def __init__(self, data_source: Optional[str] = None):
        self.storage_client = CloudStorageClient()
        self.data_source = data_source

    def process_data(self, bronze_s3_path: str) -> BronzeToSilverProcessingMetadata:
        metadata = BronzeToSilverProcessingMetadata.create(input_raw_file=bronze_s3_path, chunks_size=DEFAULT_CHUNK_SIZE)
        chunker = Chunker(metadata=metadata)
        # chunks the data and archives the original
        # saves the metadata which contains the list of chunks
        return chunker.run()

    def get_data_to_process(self) -> Iterable[dict]:
        # checks for new raw bronze data to chunk
        for bronze_s3_path in self.storage_client.get_files(prefix=BRONZE_DATA_FOLDER, regex_pattern=BRONZE_DATA_FILE_PATTERN):
            # yields the data to process
            if self.data_source:
                data_model = ModelMapper.get_data_model(bronze_s3_path)
                if not data_model or data_model.data_source != self.data_source:
                    continue
            yield {
                'bronze_s3_path': bronze_s3_path,
            }

    def run(self, show_output: bool = False) -> str:
        for task in self.get_data_to_process():
            metadata: BronzeToSilverProcessingMetadata = self.process_data(**task)
            print(metadata)
            if show_output:
                for s3_path in metadata.raw_chunks:
                    table = CloudFileReader(s3_path=s3_path).read_table()
                    print('-' * 30)
                    print(f'Chunk: {s3_path}')
                    print('-' * 30)
                    print(table)


def process_task(task_dict: dict) -> dict:
    job = ChunkBronzeJob()
    metadata: BronzeToSilverProcessingMetadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = ChunkBronzeJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = ChunkBronzeJob()
    job.run()
