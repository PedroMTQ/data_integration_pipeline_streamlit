from __future__ import annotations

from abc import abstractmethod
from typing import Iterable

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import SILVER_DATA_FOLDER


class BaseDeltaMaintenanceJob:
    """Shared discovery/config/metadata logic for optimize & vacuum jobs."""

    def get_data_to_process(self) -> Iterable[str]:
        storage_client = CloudStorageClient()
        for delta_table in storage_client.get_delta_tables(prefix=SILVER_DATA_FOLDER):
            yield delta_table

    @abstractmethod
    def process_data(self, delta_table: str):
        raise NotImplementedError()

    def run(self) -> list:
        results = []
        for delta_table in self.get_data_to_process():
            result = self.process_data(delta_table=delta_table)
            results.append(result)
            logger.info(f'Delta maintenance completed for {delta_table}')
        return results
