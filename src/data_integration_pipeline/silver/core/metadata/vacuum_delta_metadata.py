from __future__ import annotations

import os
from typing import ClassVar, Optional

from pydantic import Field

from data_integration_pipeline.common.core.base_metadata import BaseMetadata
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.settings import DELTA_VACUUM_RETENTION_HOURS, METADATA_FILE_NAME


class VacuumDeltaMetadata(BaseMetadata):
    context: ClassVar[str] = 'vacuum_delta'
    data_source: str
    delta_table: str
    retention_hours: Optional[int] = Field(default=DELTA_VACUUM_RETENTION_HOURS)
    vacuum_files_removed: int = 0
    error_message: Optional[str] = None

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(self.context, self.data_source, self.run_id, METADATA_FILE_NAME)

    @classmethod
    def create(
        cls,
        delta_table: str,
        retention_hours: Optional[int] = None,
    ) -> 'VacuumDeltaMetadata':
        metadata = BaseMetadata()
        data_model = ModelMapper.get_data_model(delta_table)
        if not data_model:
            raise ValueError(f'No data model found for {delta_table}')
        data_source = data_model.data_source
        return cls(
            **metadata.model_dump(),
            delta_table=delta_table,
            data_source=data_source,
            retention_hours=retention_hours,
        )

    def __str__(self) -> str:
        return '\n'.join(
            [
                i
                for i in [
                    f'{"─" * 60}',
                    f'🧠 Context:\t{self.context}',
                    f'🚀 Run ID:\t{self.run_id}',
                    f'📂 Delta table:\t{self.delta_table}',
                    f'ℹ️  Data source:\t{self.data_source}',
                    f'📂 Retention hours:\t{self.retention_hours}',
                    f'📊 Vacuum files removed:\t{self.vacuum_files_removed}',
                    f'📊 Error message:\t{self.error_message}' if self.error_message else None,
                    f'{"─" * 60}',
                ]
                if i
            ]
        )
