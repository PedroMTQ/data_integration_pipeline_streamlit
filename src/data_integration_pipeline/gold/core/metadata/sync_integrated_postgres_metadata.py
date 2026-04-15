from __future__ import annotations

import os
from typing import Optional


from data_integration_pipeline.gold.core.metadata.db_backend_config import PostgresBackendConfig
from data_integration_pipeline.gold.core.metadata.sync_base_metadata import SyncBaseMetadata
from data_integration_pipeline.settings import (
    POSTGRES_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from data_integration_pipeline.settings import GOLD_DATA_FOLDER, METADATA_FILE_NAME


class SyncIntegratedPostgresMetadata(SyncBaseMetadata):
    context: str = 'sync_integrated_postgres'
    input_delta_table: str
    output_postgres_table: str
    output_db_backend_config: PostgresBackendConfig

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(
            GOLD_DATA_FOLDER,
            self.context,
            self.output_postgres_table,
            self.run_id,
            METADATA_FILE_NAME,
        )

    @classmethod
    def create(
        cls,
        input_delta_table: str,
        output_postgres_table: str,
        last_synced_item: Optional[str] = None,
    ) -> SyncIntegratedPostgresMetadata:
        base = SyncBaseMetadata(last_synced_item=last_synced_item)
        output_config = PostgresBackendConfig(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DATABASE,
        )
        return cls(
            **base.model_dump(),
            input_delta_table=input_delta_table,
            output_postgres_table=output_postgres_table,
            output_db_backend_config=output_config,
        )

    @classmethod
    def get_latest(cls, output_postgres_table: str) -> Optional[SyncIntegratedPostgresMetadata]:
        return cls._get_latest(metadata_prefix=os.path.join(GOLD_DATA_FOLDER, 'sync_integrated_postgres', output_postgres_table))

    def __str__(self) -> str:
        if self.end_timestamp:
            return '\n'.join(
                i
                for i in [
                    f'{"─" * 60}',
                    f'🧠 Context:\t{self.context}',
                    f'🚀 Run ID:\t{self.run_id}',
                    f'📂 Input delta table:\t{self.input_delta_table}',
                    f'📂 Output postgres table:\t{self.output_postgres_table}',
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
                    f'📂 Input delta table:\t{self.input_delta_table}',
                    f'📂 Output postgres table:\t{self.output_postgres_table}',
                    f'⚙️  Output DB backend:\t{self.output_db_backend_config}',
                    f'🔖 Last synced item:\t{self.last_synced_item}',
                    f'📅 Start timestamp:\t{self.start_timestamp}',
                    f'{"─" * 60}',
                ]
                if i
            )
