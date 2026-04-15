import os
from itertools import batched
from pathlib import Path
from typing import Iterable

import pyarrow as pa
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.common.core.timed_trigger import TimedTrigger

from data_integration_pipeline.bronze.core.metadata import BronzeToSilverProcessingMetadata, ChunkProcessingMetadata
from data_integration_pipeline.common.core.metrics import log_metrics
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.common.io.cloud_storage import CloudFileReader, CloudFileWriter, CloudStorageClient
from data_integration_pipeline.settings import DELTA_CLIENT_BATCH_SIZE, LOG_DELAY

from data_integration_pipeline.settings import ERRORS_PREFIX


class BronzeProcessor:
    """Validates a single bronze chunk against its Pydantic data model and writes a processed parquet file."""

    def __init__(self, parent_metadata: BronzeToSilverProcessingMetadata, chunk_metadata: ChunkProcessingMetadata):
        self.parent_metadata = parent_metadata
        self.chunk_metadata = chunk_metadata
        self.storage_client = CloudStorageClient()

    @staticmethod
    def archived_errors_parquet_basename(input_chunk_s3_path: str) -> str:
        name = Path(input_chunk_s3_path).name
        if name.startswith(ERRORS_PREFIX):
            return name[len(ERRORS_PREFIX) :]
        return name

    def run(self) -> ChunkProcessingMetadata:
        """Validate the chunk, write a processed parquet file, persist chunk metadata, and delete the raw chunk."""

        data_model = ModelMapper.get_data_model(self.parent_metadata.input_raw_file)
        if not data_model:
            raise ValueError(f'No data model found for {self.parent_metadata.input_raw_file}')
        errors_s3_path = os.path.join(
            Path(self.parent_metadata.archived_raw_file).parent,
            'errors',
            BronzeProcessor.archived_errors_parquet_basename(self.chunk_metadata.input_chunk_s3_path),
        )

        logger.info(
            f'Reading raw data chunk from {self.chunk_metadata.input_chunk_s3_path} and writing processed data to {self.chunk_metadata.output_processed_s3_path}'
        )
        reader = CloudFileReader(s3_path=self.chunk_metadata.input_chunk_s3_path, as_table=False)
        fail_writer = CloudFileWriter(s3_path=errors_s3_path)
        timed_trigger = TimedTrigger(delay=LOG_DELAY, function=log_metrics)

        def validated_stream(stream_in, fail_stream_out) -> Iterable[dict]:
            for row in stream_in:
                try:
                    record_dict = data_model.from_raw_record(raw_record=row).silver_record.model_dump()
                    self.chunk_metadata.metrics.log_result(is_success=True)
                    yield record_dict
                except Exception as e:
                    logger.warning(f'Validation error: {e}')
                    fail_stream_out.write_row(row)
                    self.chunk_metadata.metrics.log_result(is_success=False)
                timed_trigger.trigger(metrics=self.chunk_metadata.metrics)

        with CloudFileWriter(s3_path=self.chunk_metadata.output_processed_s3_path) as processed_writer:
            with reader as stream_in, fail_writer as fail_stream_out:
                for batch in batched(validated_stream(stream_in, fail_stream_out), n=DELTA_CLIENT_BATCH_SIZE, strict=False):
                    try:
                        data = pa.Table.from_pylist(batch, schema=data_model._pa_silver_schema)
                    except Exception as e:
                        print(batch)
                        raise e
                    processed_writer.write_table(data)

        logger.info(f'Processed {self.chunk_metadata.input_chunk_s3_path} -> {self.chunk_metadata.output_processed_s3_path}')
        if self.chunk_metadata.metrics.failures:
            logger.warning(f'Wrote {self.chunk_metadata.metrics.failures} invalid rows to {errors_s3_path}')

        self.chunk_metadata.errors_s3_path = errors_s3_path if self.chunk_metadata.metrics.failures else None
        if self.chunk_metadata.metrics.success > 0:
            self.chunk_metadata.complete()
        else:
            logger.info(f'Skipping chunk metadata write (no valid rows) for {self.chunk_metadata.input_chunk_s3_path}')
        self.storage_client.delete_file(self.chunk_metadata.input_chunk_s3_path)
        return self.chunk_metadata
