from typing import ClassVar, Optional

from pydantic import Field, computed_field

from data_integration_pipeline.common.core.base_metadata import BaseMetadata
from data_integration_pipeline.common.core.metrics import Metrics
from data_integration_pipeline.settings import METADATA_FILE_NAME, PARQUET_SUFFIX, PROCESSED_CHUNK_SUFFIX


class ChunkProcessingMetadata(BaseMetadata):
    context: ClassVar[str] = 'chunk_processing'
    parent_metadata_s3_path: str
    data_source: str
    input_chunk_s3_path: str
    errors_s3_path: Optional[str] = None
    metrics: Metrics = Field(default_factory=Metrics)

    @property
    def metadata_s3_path(self) -> str:
        return self.input_chunk_s3_path.replace(PARQUET_SUFFIX, f'_{METADATA_FILE_NAME}')

    @computed_field
    def output_processed_s3_path(self) -> str:
        return self.input_chunk_s3_path.replace(PARQUET_SUFFIX, f'{PROCESSED_CHUNK_SUFFIX}{PARQUET_SUFFIX}')

    @classmethod
    def create(cls, input_chunk_s3_path: str, parent_metadata_s3_path: str = None, data_source: str = None) -> 'ChunkProcessingMetadata':
        base = BaseMetadata()
        return cls(
            **base.model_dump(),
            data_source=data_source,
            input_chunk_s3_path=input_chunk_s3_path,
            parent_metadata_s3_path=parent_metadata_s3_path,
        )

    def __str__(self) -> str:
        if self.end_timestamp:
            return '\n'.join(
                [
                    i
                    for i in [
                        f'{"─" * 60}',
                        f'🧠 Context:\t{self.context}',
                        f'🚀 Run ID:\t{self.run_id}',
                        f'📂 Input chunk:\t{self.input_chunk_s3_path}',
                        f'ℹ️  Data source:\t{self.data_source}',
                        f'📂 Output processed:\t{self.output_processed_s3_path}',
                        f'❌  Errors:\t{self.errors_s3_path}' if self.errors_s3_path else None,
                        f'📅 Start timestamp:\t{self.start_timestamp}',
                        f'📅 End timestamp:\t{self.end_timestamp}',
                        f'📊 Metrics:\n{self.metrics}',
                        f'{"─" * 60}',
                    ]
                    if i
                ]
            )
        else:
            return '\n'.join(
                [
                    f'{"─" * 60}',
                    f'🧠 Context:\t{self.context}',
                    f'🚀 Run ID:\t{self.run_id}',
                    f'📂 Input chunk:\t{self.input_chunk_s3_path}',
                    f'ℹ️  Data source:\t{self.data_source}',
                    f'📅 Start timestamp:\t{self.start_timestamp}',
                    f'{"─" * 60}',
                ]
            )


if __name__ == '__main__':
    metadata = ChunkProcessingMetadata.create(
        input_chunk_s3_path='s3://test/test.parquet', parent_metadata_s3_path='s3://test/test.json', data_source='test'
    )
    print(metadata.model_dump_json())
