from __future__ import annotations

import os
from typing import Any, ClassVar, Optional

from pydantic import Field

from data_integration_pipeline.common.core.base_metadata import BaseMetadata
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.settings import (
    DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS,
    DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS,
    DELTA_OPTIMIZE_TARGET_SIZE,
    DELTA_OPTIMIZE_Z_ORDER_COLUMNS,
    METADATA_FILE_NAME,
)


class OptimizeDeltaMetadata(BaseMetadata):
    context: ClassVar[str] = 'optimize_delta'
    data_source: str
    delta_table: str
    target_size: Optional[int] = Field(default=DELTA_OPTIMIZE_TARGET_SIZE)
    max_concurrent_tasks: Optional[int] = Field(default=DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS)
    min_commit_interval_seconds: Optional[int] = Field(default=DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS)
    z_order_columns: Optional[list[str]] = Field(default=DELTA_OPTIMIZE_Z_ORDER_COLUMNS)
    optimize_metrics: dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(self.context, self.data_source, self.run_id, METADATA_FILE_NAME)

    @classmethod
    def create(
        cls,
        delta_table: str,
        target_size: Optional[int],
        max_concurrent_tasks: Optional[int],
        min_commit_interval_seconds: Optional[int],
        z_order_columns: Optional[list[str]],
    ) -> 'OptimizeDeltaMetadata':
        metadata = BaseMetadata()
        data_model = ModelMapper.get_data_model(delta_table)
        if not data_model:
            raise ValueError(f'No data model found for {delta_table}')
        data_source = data_model.data_source
        return cls(
            **metadata.model_dump(),
            delta_table=delta_table,
            data_source=data_source,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            min_commit_interval_seconds=min_commit_interval_seconds,
            z_order_columns=list(z_order_columns) if z_order_columns else None,
        )

    def __get_metrics_str(self) -> str:
        return '\n'.join([f'\t{key}:\t{value}' for key, value in self.optimize_metrics.items()])

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
                    f'📂 Target size:\t{self.target_size}',
                    f'📂 Max concurrent tasks:\t{self.max_concurrent_tasks}',
                    f'📂 Min commit interval seconds:\t{self.min_commit_interval_seconds}',
                    f'📂 Z order columns:\t{self.z_order_columns}',
                    f'📊 Optimize metrics:\t{self.__get_metrics_str()}',
                    f'📊 Error message:\t{self.error_message}' if self.error_message else None,
                    f'{"─" * 60}',
                ]
                if i
            ]
        )
