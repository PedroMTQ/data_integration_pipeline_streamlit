import os

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata
from data_integration_pipeline.common.io.cloud_storage import CloudFileReader, CloudFileWriter, CloudStorageClient
from data_integration_pipeline.settings import CHUNKED_BRONZE_DATA_FOLDER, DEFAULT_CHUNK_SIZE, PARQUET_SUFFIX


class Chunker:
    def __init__(self, metadata: BronzeToSilverProcessingMetadata):
        self.metadata = metadata
        self.storage_client = CloudStorageClient()

    def run(self) -> BronzeToSilverProcessingMetadata:
        bronze_s3_path = self.metadata.input_raw_file
        chunked_bronze_s3_folder = os.path.join(CHUNKED_BRONZE_DATA_FOLDER, self.metadata.data_source, self.metadata.run_id)
        logger.info(f'Reading raw data from {bronze_s3_path}')
        reader = CloudFileReader(s3_path=bronze_s3_path, bucket_name=self.metadata.bucket_name, chunk_size=DEFAULT_CHUNK_SIZE)
        with reader as stream_in:
            for chunk_idx, chunk in enumerate(stream_in, start=1):
                chunked_bronze_s3_path = os.path.join(chunked_bronze_s3_folder, f'chunk_{chunk_idx}{PARQUET_SUFFIX}')
                logger.info(f'Writing chunk {chunk_idx} to {chunked_bronze_s3_path}')
                with CloudFileWriter(s3_path=chunked_bronze_s3_path) as stream_out:
                    stream_out.write_table(chunk)
                self.metadata.raw_chunks.append(chunked_bronze_s3_path)
        self.storage_client.move_file(current_path=bronze_s3_path, new_path=self.metadata.archived_raw_file)
        self.metadata.save()
        return self.metadata
