import os
from pathlib import Path
from typing import ClassVar, Optional

from pydantic import Field, computed_field

from data_integration_pipeline.common.core.base_metadata import BaseMetadata
from data_integration_pipeline.common.core.metrics import Metrics
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.settings import DATA_BUCKET, DELTA_TABLE_SUFFIX, METADATA_FILE_NAME, SILVER_DATA_FOLDER


class BronzeToSilverProcessingMetadata(BaseMetadata):
    context: ClassVar[str] = 'bronze_to_silver'
    input_raw_file: str
    data_source: str
    chunks_size: int
    bucket_name: str = DATA_BUCKET
    archived_raw_file: str = None
    raw_chunks: Optional[list[str]] = Field(default_factory=list)
    processed_chunks: Optional[list[str]] = Field(default_factory=list)
    errors_s3_path: Optional[list[str]] = Field(default_factory=list)
    requeued_raw_file: Optional[str] = Field(default=None)
    metrics: Metrics = Field(default_factory=Metrics)

    @computed_field
    def output_delta_table(self) -> str:
        return os.path.join(SILVER_DATA_FOLDER, self.data_source, f'records{DELTA_TABLE_SUFFIX}')

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(Path(self.archived_raw_file).parent, METADATA_FILE_NAME)

    @classmethod
    def create(
        cls,
        input_raw_file: str,
        chunks_size: int,
        bucket_name: str = DATA_BUCKET,
    ) -> 'BronzeToSilverProcessingMetadata':
        metadata = BaseMetadata()
        data_model = ModelMapper.get_data_model(input_raw_file)
        if not data_model:
            raise ValueError(f'No data model found for {input_raw_file}')
        data_source = data_model.data_source
        original_path = Path(input_raw_file)
        archived_raw_file = f'archived_{original_path.parent / metadata.run_id / original_path.name}'
        return cls(
            **metadata.model_dump(),
            input_raw_file=input_raw_file,
            data_source=data_source,
            chunks_size=chunks_size,
            archived_raw_file=archived_raw_file,
            bucket_name=bucket_name,
        )

    @computed_field
    def n_chunks(self) -> int:
        return len(self.raw_chunks)

    def __str__(self) -> str:
        if self.end_timestamp:
            return '\n'.join(
                [
                    i
                    for i in [
                        f'{"─" * 60}',
                        f'🧠 Context:\t{self.context}',
                        f'🚀 Run ID:\t{self.run_id}',
                        f'📂 Bucket name:\t{self.bucket_name}',
                        f'📂 Input:\t{self.input_raw_file}',
                        f'ℹ️  Data source:\t{self.data_source}',
                        f'ℹ️  Chunks size:\t{self.chunks_size}',
                        f'📂 # Chunks:\t{self.n_chunks}',
                        f'📂 Delta table:\t{self.output_delta_table}',
                        f'📂 Archive:\t{self.archived_raw_file}',
                        f'🔁 Requeued raw file:\t{self.requeued_raw_file}' if self.requeued_raw_file else None,
                        f'📅 Start timestamp:\t{self.start_timestamp}',
                        f'📅 End timestamp:\t{self.end_timestamp}',
                        f'📊 Metrics:\n{self.metrics}',
                        f'{self.format_list("📂 Raw chunks", self.raw_chunks)}',
                        f'{self.format_list("📂 Processed chunks", self.processed_chunks)}',
                        f'{self.format_list("📂 Errors", self.errors_s3_path)}',
                        f'{"─" * 60}',
                    ]
                    if i
                ]
            )
        else:
            return '\n'.join(
                [
                    i
                    for i in [
                        f'{"─" * 60}',
                        f'🧠 Context:\t{self.context}',
                        f'🚀 Run ID:\t{self.run_id}',
                        f'📂 Bucket name:\t{self.bucket_name}',
                        f'📂 Input:\t{self.input_raw_file}',
                        f'ℹ️  Data source:\t{self.data_source}',
                        f'ℹ️  Chunks size:\t{self.chunks_size}',
                        f'📂 # Chunks:\t{self.n_chunks}',
                        f'📂 Delta table:\t{self.output_delta_table}',
                        f'📂 Archive:\t{self.archived_raw_file}',
                        f'🔁 Requeued raw file:\t{self.requeued_raw_file}' if self.requeued_raw_file else None,
                        f'📅 Start timestamp:\t{self.start_timestamp}',
                        f'{self.format_list("📂 Raw chunks", self.raw_chunks)}',
                        f'{self.format_list("📂 Processed chunks", self.processed_chunks)}',
                        f'{self.format_list("📂 Errors", self.errors_s3_path)}',
                        f'{"─" * 60}',
                    ]
                    if i
                ]
            )
