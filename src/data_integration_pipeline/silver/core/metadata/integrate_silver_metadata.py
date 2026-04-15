from __future__ import annotations

import os
from typing import Literal, Optional


from data_integration_pipeline.gold.core.metadata.sync_base_metadata import SyncBaseMetadata
from data_integration_pipeline.settings import METADATA_FILE_NAME, SILVER_DATA_FOLDER


class IntegrateSilverMetadata(SyncBaseMetadata):
    context: Literal['integrate_silver'] = 'integrate_silver'
    seed_data_source: str
    input_delta_tables: list[str]
    output_delta_table: str

    @property
    def metadata_s3_path(self) -> str:
        return os.path.join(SILVER_DATA_FOLDER, self.context, self.run_id, METADATA_FILE_NAME)

    @classmethod
    def create(
        cls,
        seed_data_source: str,
        input_delta_tables: list[str],
        output_delta_table: str,
    ) -> 'IntegrateSilverMetadata':
        from data_integration_pipeline.common.core.base_metadata import BaseMetadata

        base = BaseMetadata()
        return cls(
            **base.model_dump(),
            seed_data_source=seed_data_source,
            input_delta_tables=input_delta_tables,
            output_delta_table=output_delta_table,
        )

    @classmethod
    def get_latest(cls) -> Optional['IntegrateSilverMetadata']:
        return cls._get_latest(metadata_prefix=os.path.join(SILVER_DATA_FOLDER, 'integrate_silver'))

    def __str__(self) -> str:
        tables_str = ', '.join(self.input_delta_tables)
        if self.end_timestamp:
            return '\n'.join(
                i
                for i in [
                    f'{"─" * 60}',
                    f'🧠 Context:\t{self.context}',
                    f'🚀 Run ID:\t{self.run_id}',
                    f'📂 Input tables:\t{tables_str}',
                    f'ℹ️  Seed data source:\t{self.seed_data_source}',
                    f'📂 Output table:\t{self.output_delta_table}',
                    f'📊 # Input records:\t{self.n_input_records}',
                    f'📊 # Output records:\t{self.n_output_records}',
                    f'✅ Sync finished:\t{self.sync_finished}',
                    f'📅 Start timestamp:\t{self.start_timestamp}',
                    f'📅 End timestamp:\t{self.end_timestamp}',
                    f'📊 Metrics:\n{self.metrics}',
                    f'{"─" * 60}',
                ]
                if i
            )
        return '\n'.join(
            i
            for i in [
                f'{"─" * 60}',
                f'🧠 Context:\t{self.context}',
                f'🚀 Run ID:\t{self.run_id}',
                f'📂 Input tables:\t{tables_str}',
                f'ℹ️  Seed data source:\t{self.seed_data_source}',
                f'📂 Output table:\t{self.output_delta_table}',
                f'📅 Start timestamp:\t{self.start_timestamp}',
                f'{"─" * 60}',
            ]
            if i
        )


if __name__ == '__main__':
    metadata = IntegrateSilverMetadata.create(
        seed_data_source='dataset_1',
        input_delta_tables=[
            'silver/dataset_1/records.delta',
            'silver/dataset_2/records.delta',
            'silver/dataset_3/records.delta',
        ],
        output_delta_table='silver/integrated/records.delta',
    )
    print(metadata)
