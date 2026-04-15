from __future__ import annotations

import os
from typing import Literal, Optional

from pydantic import Field

from data_integration_pipeline.gold.core.metadata.db_backend_config import ElasticsearchBackendConfig, PostgresBackendConfig
from data_integration_pipeline.gold.core.metadata.sync_base_metadata import LastSyncedItem, SyncBaseMetadata
from data_integration_pipeline.settings import (
    ELASTICSEARCH_INDEX_ALIAS,
    ELASTICSEARCH_INDEX_NAME,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_USER,
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from data_integration_pipeline.settings import GOLD_DATA_FOLDER, METADATA_FILE_NAME


class SyncElasticSearchMetadata(SyncBaseMetadata):
    context: Literal['sync_elastic_search'] = 'sync_elastic_search'
    input_postgres_table: str
    output_es_index: str
    input_db_backend_config: PostgresBackendConfig
    output_db_backend_config: ElasticsearchBackendConfig

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(GOLD_DATA_FOLDER, self.context, self.output_es_index, self.run_id, METADATA_FILE_NAME)

    @classmethod
    def create(
        cls,
        input_postgres_table: str,
        output_es_index: str,
        last_synced_item: Optional[LastSyncedItem] = None,
    ) -> 'SyncElasticSearchMetadata':
        metadata = SyncBaseMetadata(last_synced_item=last_synced_item)
        input_db_backend_config = PostgresBackendConfig(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DATABASE,
        )
        output_db_backend_config = ElasticsearchBackendConfig(
            urls=[ELASTICSEARCH_URL],
            user=ELASTICSEARCH_USER,
            password=ELASTICSEARCH_PASSWORD,
            index_name=ELASTICSEARCH_INDEX_NAME,
            index_alias=ELASTICSEARCH_INDEX_ALIAS,
        )
        return cls(
            **metadata.model_dump(),
            input_postgres_table=input_postgres_table,
            output_es_index=output_es_index,
            input_db_backend_config=input_db_backend_config,
            output_db_backend_config=output_db_backend_config,
        )

    @classmethod
    def get_latest(cls, es_index_name: str) -> Optional['SyncElasticSearchMetadata']:
        """Load the most recent metadata from S3 (by start_timestamp), including incomplete runs."""
        return cls._get_latest(metadata_prefix=os.path.join(GOLD_DATA_FOLDER, 'sync_elastic_search', es_index_name))

    def __str__(self) -> str:
        if self.end_timestamp:
            return '\n'.join(
                i
                for i in [
                    f'{"─" * 60}',
                    f'🧠 Context:\t{self.context}',
                    f'🚀 Run ID:\t{self.run_id}',
                    f'⚙️  Input DB backend:\t{self.input_db_backend_config}',
                    f'⚙️  Output DB backend:\t{self.output_db_backend_config}',
                    f'📂 Metadata:\t{self.metadata_s3_path}',
                    f'📊 # Input records:\t{self.n_input_records}',
                    f'📊 # Output records:\t{self.n_output_records}',
                    f'🔖 Last synced item:\t{self.last_synced_item}',
                    f'✅ Sync finished:\t{self.sync_finished}',
                    f'📅 Start timestamp:\t{self.start_timestamp}',
                    f'📅 End timestamp:\t{self.end_timestamp}',
                    f'📊 Metrics:\n{self.metrics}',
                    f'{"─" * 60}',
                ]
                if i
            )
        else:
            return '\n'.join(
                i
                for i in [
                    f'{"─" * 60}',
                    f'🧠 Context:\t{self.context}',
                    f'🚀 Run ID:\t{self.run_id}',
                    f'⚙️  Input DB backend:\t{self.input_db_backend_config}',
                    f'⚙️  Output DB backend:\t{self.output_db_backend_config}',
                    f'🔖 Last synced item:\t{self.last_synced_item}',
                    f'📅 Start timestamp:\t{self.start_timestamp}',
                    f'{"─" * 60}',
                ]
                if i
            )
