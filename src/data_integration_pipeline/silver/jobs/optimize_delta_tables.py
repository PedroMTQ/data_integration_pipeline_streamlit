from __future__ import annotations

from datetime import timedelta

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.common.io.delta_client import DeltaClient
from data_integration_pipeline.settings import (
    DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS,
    DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS,
    DELTA_OPTIMIZE_TARGET_SIZE,
    DELTA_OPTIMIZE_Z_ORDER_COLUMNS,
)
from data_integration_pipeline.silver.core.metadata.optimize_delta_metadata import OptimizeDeltaMetadata
from data_integration_pipeline.silver.jobs._delta_maintenance_base import BaseDeltaMaintenanceJob


class OptimizeDeltaTablesJob(BaseDeltaMaintenanceJob):
    """Runs optimize operations on discovered Delta tables."""

    def process_data(self, delta_table: str) -> OptimizeDeltaMetadata:
        metadata = OptimizeDeltaMetadata.create(
            delta_table=delta_table,
            target_size=DELTA_OPTIMIZE_TARGET_SIZE,
            max_concurrent_tasks=DELTA_OPTIMIZE_MAX_CONCURRENT_TASKS,
            min_commit_interval_seconds=DELTA_OPTIMIZE_MIN_COMMIT_INTERVAL_SECONDS,
            z_order_columns=DELTA_OPTIMIZE_Z_ORDER_COLUMNS,
        )
        logger.info(f'Optimizing Delta table: {delta_table}')
        delta_client = DeltaClient()

        try:
            metadata.optimize_metrics = delta_client.optimize_table(
                table_name=delta_table,
                target_size=metadata.target_size,
                max_concurrent_tasks=metadata.max_concurrent_tasks,
                min_commit_interval=timedelta(seconds=metadata.min_commit_interval_seconds),
                z_order_columns=metadata.z_order_columns,
            )
        except Exception as e:
            logger.exception(f'Optimize failed for {delta_table}')
            metadata.error_message = str(e)
        finally:
            metadata.complete(metadata.metadata_s3_path)
        logger.info(f'Optimized Delta table:\n{metadata}')
        return metadata


def process_task(task_dict: dict) -> dict:
    job = OptimizeDeltaTablesJob()
    metadata: OptimizeDeltaMetadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = OptimizeDeltaTablesJob()
    return [{'delta_table': delta_table} for delta_table in job.get_data_to_process()]


if __name__ == '__main__':
    OptimizeDeltaTablesJob().run()
