from typing import Iterable

from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.common.io.delta_client import DeltaClient
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.silver.core.integrate_silver_processor import IntegrateSilverProcessor
from data_integration_pipeline.silver.core.metadata.integrate_silver_metadata import IntegrateSilverMetadata


class IntegrateSilverJob:
    """Joins the three per-source silver Delta tables into one integrated silver Delta table."""

    INPUT_DELTA_TABLES = [
        'silver/dataset_1/records.delta',
        'silver/dataset_2/records.delta',
        'silver/dataset_3/records.delta',
    ]
    OUTPUT_DELTA_TABLE = 'silver/integrated/records.delta'

    def process_data(self, seed_data_source: str, input_delta_tables: list[str], output_delta_table: str) -> IntegrateSilverMetadata:
        """Joins the three per-source silver Delta tables into one integrated silver Delta table."""
        metadata = IntegrateSilverMetadata.create(
            seed_data_source=seed_data_source,
            input_delta_tables=input_delta_tables,
            output_delta_table=output_delta_table,
        )
        logger.info(f'Starting silver integration via \n{metadata}')
        processor = IntegrateSilverProcessor(metadata=metadata)
        processor.run()
        metadata.complete(metadata.metadata_s3_path)
        return metadata

    def _output_is_stale(self) -> bool:
        """True when any input table was committed more recently than the output table."""
        delta_client = DeltaClient()
        output_ts = delta_client.get_last_commit_timestamp(self.OUTPUT_DELTA_TABLE)
        if output_ts is None:
            return True
        for input_table in self.INPUT_DELTA_TABLES:
            input_ts = delta_client.get_last_commit_timestamp(input_table)
            if input_ts is not None and input_ts > output_ts:
                logger.info(f'{input_table} has newer commits than {self.OUTPUT_DELTA_TABLE}, re-integration needed')
                return True
        return False

    def get_data_to_process(self) -> Iterable[dict]:
        storage_client = CloudStorageClient()
        delta_tables = storage_client.get_delta_tables(prefix='silver')
        data_model = ModelMapper.get_data_model(self.OUTPUT_DELTA_TABLE)
        if not data_model:
            raise ValueError(f'No data model found for {self.OUTPUT_DELTA_TABLE}')
        seed_data_source = data_model._silver_schema._seed_data_source
        all_exist = all(delta_table in delta_tables for delta_table in self.INPUT_DELTA_TABLES)
        if not all_exist:
            return
        if not self._output_is_stale():
            logger.info(f'{self.OUTPUT_DELTA_TABLE} is up-to-date with all input tables, skipping integration')
            return
        yield {
            'seed_data_source': seed_data_source,
            'input_delta_tables': self.INPUT_DELTA_TABLES,
            'output_delta_table': self.OUTPUT_DELTA_TABLE,
        }

    def run(self):
        for task_data in self.get_data_to_process():
            metadata: IntegrateSilverMetadata = self.process_data(**task_data)
            logger.info(f'Finished task: {metadata}')


def process_task(task_dict: dict) -> dict:
    job = IntegrateSilverJob()
    metadata = job.process_data(**task_dict)
    logger.info(f'Finished task: {metadata}')
    return metadata.model_dump()


def get_tasks() -> list[dict]:
    job = IntegrateSilverJob()
    return list(job.get_data_to_process())


if __name__ == '__main__':
    job = IntegrateSilverJob()
    job.run()
