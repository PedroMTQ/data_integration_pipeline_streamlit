from typing import Iterable, Optional

from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.gold.core.metadata.sync_integrated_postgres_metadata import SyncIntegratedPostgresMetadata
from data_integration_pipeline.gold.core.sync_integrated_postgres_processor import SyncIntegratedPostgresProcessor
from data_integration_pipeline.common.io.delta_client import DeltaClient
from data_integration_pipeline.gold.io.postgres_client import PostgresClient
from data_integration_pipeline.gold.io import get_db_client


class SyncPostgresJob:
    """Thin orchestrator: creates metadata, delegates to SyncIntegratedPostgresProcessor."""

    INPUT_DELTA_TABLE = 'silver/integrated/records.delta'
    OUTPUT_POSTGRES_TABLE = 'integrated_records'

    @staticmethod
    def _resolve_last_synced_item() -> Optional[str]:
        previous = SyncIntegratedPostgresMetadata.get_latest(output_postgres_table=SyncPostgresJob.OUTPUT_POSTGRES_TABLE)
        if previous is None:
            return None
        if previous.sync_finished:
            return None
        return previous.last_synced_item

    def process_data(self, input_delta_table: str, output_postgres_table: str) -> SyncIntegratedPostgresMetadata:
        last_synced_item = self._resolve_last_synced_item()
        metadata = SyncIntegratedPostgresMetadata.create(
            input_delta_table=input_delta_table,
            output_postgres_table=output_postgres_table,
            last_synced_item=last_synced_item,
        )
        logger.info(f'Starting integrated Postgres sync:\n{metadata}')
        processor = SyncIntegratedPostgresProcessor(metadata=metadata)
        processor.run()
        metadata.complete(metadata.metadata_s3_path)
        logger.info(f'Integrated Postgres sync complete:\n{metadata}')
        return metadata

    def _output_is_stale(self) -> bool:
        """True when any input table was committed more recently than the output table."""
        delta_client = DeltaClient()
        input_ts = delta_client.get_last_commit_timestamp(self.INPUT_DELTA_TABLE)
        metadata = SyncIntegratedPostgresMetadata.create(
            input_delta_table=self.INPUT_DELTA_TABLE,
            output_postgres_table=self.OUTPUT_POSTGRES_TABLE,
        )
        postgres_client: PostgresClient = get_db_client(metadata.output_db_backend_config)
        if not postgres_client.table_exists(self.OUTPUT_POSTGRES_TABLE):
            logger.info(f'{self.OUTPUT_POSTGRES_TABLE} does not exist, running sync')
            return True
        output_ts = postgres_client.get_last_commit_timestamp(self.OUTPUT_POSTGRES_TABLE)
        if output_ts is None:
            logger.info(f'{self.OUTPUT_POSTGRES_TABLE} has no commits, running sync')
            return True
        if input_ts is not None and input_ts > output_ts:
            logger.info(f'{self.INPUT_DELTA_TABLE} has newer commits than {self.OUTPUT_POSTGRES_TABLE}, re-integration needed')
            return True
        logger.info(f'{self.INPUT_DELTA_TABLE} has older commits than {self.OUTPUT_POSTGRES_TABLE}, skipping sync')
        return False

    def get_data_to_process(self) -> Iterable[dict]:
        storage_client = CloudStorageClient()
        delta_tables = storage_client.get_delta_tables()
        if self.INPUT_DELTA_TABLE not in delta_tables:
            logger.warning(f'Integrated Delta table {self.INPUT_DELTA_TABLE!r} not found; skipping Postgres sync.')
            return
        if not self._output_is_stale():
            logger.info(f'{self.OUTPUT_POSTGRES_TABLE} is up-to-date with all input tables, skipping integration')
            return
        yield {
            'input_delta_table': self.INPUT_DELTA_TABLE,
            'output_postgres_table': self.OUTPUT_POSTGRES_TABLE,
        }

    def run(self) -> None:
        for task_data in self.get_data_to_process():
            self.process_data(**task_data)


def process_task(task_dict: dict) -> dict:
    job = SyncPostgresJob()
    metadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = SyncPostgresJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = SyncPostgresJob()
    job.run()
