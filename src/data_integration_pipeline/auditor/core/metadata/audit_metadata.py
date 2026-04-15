from __future__ import annotations

import os
from typing import ClassVar, Literal, Optional

from pydantic import Field

from data_integration_pipeline.common.core.base_metadata import BaseMetadata
from data_integration_pipeline.settings import METADATA_FILE_NAME


class AuditMetadata(BaseMetadata):
    context: ClassVar[str] = 'audit'
    s3_path: str
    dataset_stage: Literal['bronze', 'silver', 'gold']
    error_message: Optional[str] = None
    total_raw_records: Optional[int] = None
    total_sampled_records: Optional[int] = None
    raw_data_distribution: dict[str, int] = Field(default_factory=dict)
    sample_data_distribution: dict[str, int] = Field(default_factory=dict)
    audit_columns: list[str] = Field(default_factory=list)

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(self.context, self.s3_path, self.run_id, METADATA_FILE_NAME)

    @classmethod
    def create(
        cls,
        s3_path: str,
        dataset_stage: Literal['bronze', 'silver', 'gold'],
    ) -> 'AuditMetadata':
        metadata = BaseMetadata()
        return cls(
            **metadata.model_dump(),
            s3_path=s3_path,
            dataset_stage=dataset_stage,
        )

    def __str__(self) -> str:
        return '\n'.join(
            [
                f'{"─" * 60}',
                f'🧠 Context:\t{self.context}',
                f'🚀 Run ID:\t{self.run_id}',
                f'📂 Path:\t{self.s3_path}',
                f'📂 Stage:\t{self.dataset_stage}',
                f'📊 Success:\t{self.success}',
                f'📊 Raw records:\t{self.total_raw_records}',
                f'📊 Sampled records:\t{self.total_sampled_records}',
                f'📊 Error:\t{self.error_message}',
                f'{"─" * 60}',
            ]
        )
