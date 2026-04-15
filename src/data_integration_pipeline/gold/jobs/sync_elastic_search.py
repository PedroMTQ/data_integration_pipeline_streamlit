from datetime import datetime
from typing import Iterable, Optional

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.gold.core.metadata.sync_base_metadata import LastSyncedItem
from data_integration_pipeline.gold.core.metadata.sync_elastic_search_metadata import SyncElasticSearchMetadata
from data_integration_pipeline.gold.core.sync_elastic_search_processor import SyncElasticSearchProcessor
from data_integration_pipeline.gold.io import get_db_client
from data_integration_pipeline.gold.io.postgres_client import PostgresClient


class SyncOrganizationsElasticsearchJob:
    """Thin orchestrator: creates metadata, delegates to SyncElasticSearchProcessor."""

    INPUT_POSTGRES_TABLE = 'integrated_records'
    OUTPUT_ES_INDEX = 'integrated_records'

    @staticmethod
    def _resolve_last_synced_item() -> Optional[LastSyncedItem]:
        previous = SyncElasticSearchMetadata.get_latest(es_index_name=SyncOrganizationsElasticsearchJob.OUTPUT_ES_INDEX)
        if previous is None:
            return None
        if previous.sync_finished:
            return None
        return previous.last_synced_item

    @staticmethod
    def get_last_commit_timestamp() -> Optional[datetime]:
        """Best-known watermark of what has already been synced to ES."""
        previous = SyncElasticSearchMetadata.get_latest(es_index_name=SyncOrganizationsElasticsearchJob.OUTPUT_ES_INDEX)
        if previous is None:
            return None
        if previous.end_timestamp is not None:
            return previous.end_timestamp
        return None

    def _output_is_stale(self) -> bool:
        """True when Postgres has newer rows than the last synced ES watermark."""
        metadata = SyncElasticSearchMetadata.create(
            input_postgres_table=self.INPUT_POSTGRES_TABLE,
            output_es_index=self.OUTPUT_ES_INDEX,
        )
        postgres_client: PostgresClient = get_db_client(metadata.input_db_backend_config)
        input_ts = postgres_client.get_last_commit_timestamp(self.INPUT_POSTGRES_TABLE)
        output_ts = self.get_last_commit_timestamp()
        if output_ts is None:
            return True
        if input_ts is not None and input_ts > output_ts:
            logger.info(f'{self.INPUT_POSTGRES_TABLE} has newer commits than {self.OUTPUT_ES_INDEX}, ES sync needed')
            return True
        return False

    def process_data(self, input_postgres_table: str, output_es_index: str) -> SyncElasticSearchMetadata:
        last_synced_item = self._resolve_last_synced_item()
        metadata = SyncElasticSearchMetadata.create(
            last_synced_item=last_synced_item,
            input_postgres_table=input_postgres_table,
            output_es_index=output_es_index,
        )
        logger.info(f'Starting ES index sync:\n{metadata}')
        processor = SyncElasticSearchProcessor(metadata)
        processor.run()
        metadata.complete(metadata.metadata_s3_path)
        logger.info(f'ES index sync complete:\n{metadata}')
        return metadata

    def get_data_to_process(self) -> Iterable[dict]:
        if not self._output_is_stale():
            logger.info(f'{self.OUTPUT_ES_INDEX} is up-to-date with {self.INPUT_POSTGRES_TABLE}, skipping sync')
            return
        yield {
            'input_postgres_table': self.INPUT_POSTGRES_TABLE,
            'output_es_index': self.OUTPUT_ES_INDEX,
        }

    def run(self) -> None:
        for task_data in self.get_data_to_process():
            self.process_data(**task_data)


def process_task(task_dict: dict) -> dict:
    job = SyncOrganizationsElasticsearchJob()
    metadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = SyncOrganizationsElasticsearchJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = SyncOrganizationsElasticsearchJob()
    job.run()
