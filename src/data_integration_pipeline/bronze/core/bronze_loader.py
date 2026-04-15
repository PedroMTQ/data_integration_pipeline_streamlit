from typing import Iterable

import pyarrow as pa
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata, ChunkProcessingMetadata
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.common.io.cloud_storage import CloudFileReader, CloudStorageClient
from data_integration_pipeline.common.io.delta_client import DeltaClient
from data_integration_pipeline.settings import DELTA_CLIENT_BATCH_SIZE


class BronzeLoader:
    """Writes processed parquet chunks to a silver Delta table sequentially."""

    def __init__(self, parent_metadata: BronzeToSilverProcessingMetadata, chunks_metadata: list[ChunkProcessingMetadata]):
        self.parent_metadata: BronzeToSilverProcessingMetadata = parent_metadata
        self.chunks_metadata: list[ChunkProcessingMetadata] = chunks_metadata
        self.storage_client = CloudStorageClient()
        self.delta_client = DeltaClient()

    def run(self) -> BronzeToSilverProcessingMetadata:
        """Write all processed chunks to Delta, aggregate results, and finalize parent metadata."""
        self._write_chunks_to_delta()
        self._finalize_parent_metadata()
        return self.parent_metadata

    def _stream_chunks_to_delta(self) -> Iterable[pa.Table]:
        """
        Streams chunks to Delta table in batches of DELTA_CLIENT_BATCH_SIZE.

        This function goes through several chunks until we get enough data to fill a batch of DELTA_CLIENT_BATCH_SIZE.
        Then it yields the aggregated batch and cleans up the chunks that have been processed.

        The goal here is to avoid doing too many small writes to the Delta table.
        """
        batch: list[pa.Table] = []
        current_batch_size = 0
        pending_cleanup: list[ChunkProcessingMetadata] = []
        chunk_metadata: ChunkProcessingMetadata
        for chunk_metadata in self.chunks_metadata:
            processed_path = chunk_metadata.output_processed_s3_path
            if not self.storage_client.file_exists(processed_path):
                logger.warning(f'Chunk {processed_path} not found')
                continue
            logger.info(f'Writing {processed_path} to Delta table {self.parent_metadata.output_delta_table}')
            reader = CloudFileReader(s3_path=processed_path, as_table=True, chunk_size=DELTA_CLIENT_BATCH_SIZE)
            with reader as stream:
                for chunk in stream:
                    current_batch_size += len(chunk)
                    if isinstance(chunk, pa.RecordBatch):
                        chunk = pa.Table.from_batches([chunk])
                    batch.append(chunk)
                    if current_batch_size >= DELTA_CLIENT_BATCH_SIZE:
                        yield pa.concat_tables(batch, promote_options='default')
                        for completed_chunk in pending_cleanup:
                            self._cleanup_chunk_metadata(completed_chunk)
                        pending_cleanup = []
                        batch = []
                        current_batch_size = 0
            if current_batch_size == 0:
                self._cleanup_chunk_metadata(chunk_metadata)
            else:
                pending_cleanup.append(chunk_metadata)
        if batch:
            yield pa.concat_tables(batch, promote_options='default')
            for completed_chunk in pending_cleanup:
                self._cleanup_chunk_metadata(completed_chunk)

    def _write_chunks_to_delta(self) -> None:
        data_stream = self._stream_chunks_to_delta()
        for data in data_stream:
            self.delta_client.write(
                table_name=self.parent_metadata.output_delta_table,
                data=data,
                primary_key=ModelMapper.get_primary_key(self.parent_metadata.output_delta_table),
                partition_key=ModelMapper.get_partition_key(self.parent_metadata.output_delta_table),
            )

    def _cleanup_chunk_metadata(self, chunk_metadata: ChunkProcessingMetadata):
        self.parent_metadata.processed_chunks.append(chunk_metadata.output_processed_s3_path)
        self.parent_metadata.metrics.join(chunk_metadata.metrics)
        if chunk_metadata.errors_s3_path:
            self.parent_metadata.errors_s3_path.append(chunk_metadata.errors_s3_path)
        self.parent_metadata.save()
        self.storage_client.delete_file(chunk_metadata.output_processed_s3_path)
        self.storage_client.delete_file(chunk_metadata.metadata_s3_path)
        logger.info(f'Wrote and deleted {chunk_metadata.output_processed_s3_path}')

    def _finalize_parent_metadata(self):
        has_raw = any(self.storage_client.file_exists(c) for c in self.parent_metadata.raw_chunks)
        has_processed = any(self.storage_client.file_exists(cm.output_processed_s3_path) for cm in self.chunks_metadata)
        if not has_raw and not has_processed:
            self.parent_metadata.complete()
            logger.info(f'All chunks written to Delta — marked {self.parent_metadata.metadata_s3_path} complete')
        else:
            self.parent_metadata.save()
            logger.info(f'Saved metadata {self.parent_metadata.metadata_s3_path} (still has pending chunks)')
