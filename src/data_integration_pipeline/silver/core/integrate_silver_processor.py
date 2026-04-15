from pydantic import AliasPath

from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.common.core.models.integrated.metamodel import IntegratedMetaModel
from data_integration_pipeline.common.io.spark_client import SparkClient
from data_integration_pipeline.silver.core.metadata.integrate_silver_metadata import IntegrateSilverMetadata
import pyspark
from pyspark.sql import functions as F

from data_integration_pipeline.common.core.models.model_mapper import ModelMapper


class IntegrateSilverProcessor:
    def __init__(self, metadata: IntegrateSilverMetadata):
        self.metadata = metadata
        data_model = ModelMapper.get_data_model(self.metadata.output_delta_table)
        if not data_model:
            raise ValueError(f'No data model found for {self.metadata.output_delta_table}')
        schema = data_model._silver_schema
        self.primary_key = schema._primary_key
        self.partition_key = schema._partition_key
        self.join_key = schema._join_key
        self.seed_data_source = schema._seed_data_source
        self.column_mappings = self._derive_column_mappings(schema_cls=schema, seed_data_source=self.seed_data_source)
        self.spark_client = SparkClient(app_name=self.metadata.context)

    @staticmethod
    def _derive_column_mappings(schema_cls: type, seed_data_source: str) -> list[tuple[str, str, str]]:
        """
        Returns a list of (data_source, source_column, output_column) tuples
        derived from each field's AliasPath(source, column).

        Fields without an AliasPath are mapped from ``seed_data_source``
        using the field name directly.
        """
        mappings: list[tuple[str, str, str]] = []
        for field_name, field_info in schema_cls.model_fields.items():
            alias = field_info.validation_alias
            if isinstance(alias, AliasPath) and len(alias.path) == 2:
                source, source_col = alias.path
                mappings.append((source, source_col, field_name))
            else:
                mappings.append((seed_data_source, field_name, field_name))
        return mappings

    def _read_sources(self) -> dict[str, pyspark.sql.DataFrame]:
        sources: dict[str, pyspark.sql.DataFrame] = {}
        for table_path in self.metadata.input_delta_tables:
            data_model = ModelMapper.get_data_model(table_path)
            if not data_model:
                raise ValueError(f'No data model found for {table_path}')
            data_source = data_model.data_source
            sources[data_source] = self.spark_client.read(table_path)
            logger.info(f'Read {data_source}: {sources[data_source].count()} rows')
        return sources

    def _join(self, sources: dict[str, pyspark.sql.DataFrame]) -> pyspark.sql.DataFrame:
        self.metadata.n_input_records = sources[self.seed_data_source].count()

        aliased = {data_source: data.alias(data_source) for data_source, data in sources.items()}

        select_exprs = []
        for data_source, source_col, output_col in self.column_mappings:
            col_ref = F.col(f'{data_source}.{source_col}')
            if source_col != output_col:
                select_exprs.append(col_ref.alias(output_col))
            else:
                select_exprs.append(col_ref)

        joined = aliased[self.seed_data_source]
        for data_source, data in aliased.items():
            if data_source != self.seed_data_source:
                joined = joined.join(
                    data,
                    F.col(f'{self.seed_data_source}.{self.join_key}') == F.col(f'{data_source}.{self.join_key}'),
                    'left',
                )
        joined = joined.select(*select_exprs)
        return joined

    def _validate(self, data: pyspark.sql.DataFrame) -> None:
        SparkClient.assert_schema_matches(data, IntegratedMetaModel._pa_silver_schema)
        logger.info('Schema contract validation passed')

    def _write(self, data: pyspark.sql.DataFrame) -> None:
        self.spark_client.write(
            table_name=self.metadata.output_delta_table,
            data=data,
            primary_key=self.primary_key,
            partition_key=self.partition_key,
        )
        self.metadata.n_output_records = self.spark_client.get_count(self.metadata.output_delta_table)

    def run(self) -> IntegrateSilverMetadata:
        """Orchestrates read -> join -> validate -> write for silver integration."""
        try:
            sources = self._read_sources()
            joined = self._join(sources)
            self._validate(joined)
            self._write(joined)
            self.metadata.sync_finished = True
            self.metadata.metrics.log_result(is_success=True, total=self.metadata.n_output_records or 0)
            logger.info(f'Integration complete: {self.metadata.n_input_records} input -> {self.metadata.n_output_records} output')
        except Exception:
            self.metadata.metrics.log_result(is_success=False)
            raise
        finally:
            self.spark_client.stop()
        return self.metadata
