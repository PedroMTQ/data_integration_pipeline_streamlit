from __future__ import annotations

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.common.io.delta_client import DeltaClient
from data_integration_pipeline.settings import DELTA_VACUUM_RETENTION_HOURS
from data_integration_pipeline.silver.core.metadata.vacuum_delta_metadata import VacuumDeltaMetadata
from data_integration_pipeline.silver.jobs._delta_maintenance_base import BaseDeltaMaintenanceJob


class VacuumDeltaTablesJob(BaseDeltaMaintenanceJob):
    """Runs vacuum operations on discovered Delta tables."""

    def process_data(self, delta_table: str) -> VacuumDeltaMetadata:
        metadata = VacuumDeltaMetadata.create(delta_table=delta_table, retention_hours=DELTA_VACUUM_RETENTION_HOURS)
        logger.info(f'Vacuuming Delta table: {delta_table}')
        delta_client = DeltaClient()
        try:
            vacuumed_files = delta_client.vacuum_table(
                table_name=delta_table,
                retention_hours=metadata.retention_hours,
                dry_run=False,
            )
            metadata.vacuum_files_removed = len(vacuumed_files)
        except Exception as e:
            logger.exception(f'Vacuum failed for {delta_table}')
            metadata.error_message = str(e)
        finally:
            metadata.complete(metadata.metadata_s3_path)
        logger.info(f'Vacuumed Delta table:\n{metadata}')

        return metadata


def process_task(task_dict: dict) -> dict:
    job = VacuumDeltaTablesJob()
    metadata: VacuumDeltaMetadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = VacuumDeltaTablesJob()
    return [{'delta_table': delta_table} for delta_table in job.get_data_to_process()]


if __name__ == '__main__':
    VacuumDeltaTablesJob().run()
