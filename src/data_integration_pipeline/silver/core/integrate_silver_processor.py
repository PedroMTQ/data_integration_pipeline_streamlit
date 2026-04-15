from __future__ import annotations

from typing import Any

from pydantic import AliasPath

from data_integration_pipeline.common.core.models.integrated.metamodel import IntegratedMetaModel
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.settings import DATA_BUCKET, DEFAULT_CHUNK_SIZE
from data_integration_pipeline.silver.core.metadata.integrate_silver_metadata import IntegrateSilverMetadata


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

        if self.metadata.backend_engine == 'spark':
            from data_integration_pipeline.common.io.spark_client import SparkClient

            self.spark_client = SparkClient(app_name=self.metadata.context)
        else:
            import duckdb

            from data_integration_pipeline.common.io.duckdb_client import DuckdbClient

            self.duckdb_conn = duckdb.connect()
            DuckdbClient.load_delta_scan(self.duckdb_conn)
            DuckdbClient.load_s3_connector(self.duckdb_conn)
            DuckdbClient.add_s3_secret(self.duckdb_conn)

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

    # ------------------------------------------------------------------
    # Spark backend
    # ------------------------------------------------------------------

    def _read_sources_spark(self) -> dict:
        sources = {}
        for table_path in self.metadata.input_delta_tables:
            data_model = ModelMapper.get_data_model(table_path)
            if not data_model:
                raise ValueError(f'No data model found for {table_path}')
            data_source = data_model.data_source
            sources[data_source] = self.spark_client.read(table_path)
            logger.info(f'Read {data_source}: {sources[data_source].count()} rows')
        return sources

    def _join_spark(self, sources: dict) -> Any:
        from pyspark.sql import functions as F

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
        return joined.select(*select_exprs)

    def _validate_spark(self, data: Any) -> None:
        from data_integration_pipeline.common.io.spark_client import SparkClient

        SparkClient.assert_schema_matches(data, IntegratedMetaModel._pa_silver_schema)

    def _write_spark(self, data: Any) -> None:
        self.spark_client.write(
            table_name=self.metadata.output_delta_table,
            data=data,
            primary_key=self.primary_key,
            partition_key=self.partition_key,
        )
        self.metadata.n_output_records = self.spark_client.get_count(self.metadata.output_delta_table)

    # ------------------------------------------------------------------
    # DuckDB backend
    # ------------------------------------------------------------------

    def _read_sources_duckdb(self) -> dict[str, str]:
        sources: dict[str, str] = {}
        for table_path in self.metadata.input_delta_tables:
            data_model = ModelMapper.get_data_model(table_path)
            if not data_model:
                raise ValueError(f'No data model found for {table_path}')
            data_source = data_model.data_source
            uri = f's3://{DATA_BUCKET}/{table_path}'
            self.duckdb_conn.execute(f"CREATE OR REPLACE VIEW {data_source} AS SELECT * FROM delta_scan('{uri}')")
            count = self.duckdb_conn.execute(f'SELECT COUNT(*) FROM {data_source}').fetchone()[0]
            sources[data_source] = data_source
            logger.info(f'Read {data_source}: {count} rows')
        return sources

    def _join_duckdb(self, sources: dict[str, str]) -> Any:
        self.metadata.n_input_records = self.duckdb_conn.execute(f'SELECT COUNT(*) FROM {self.seed_data_source}').fetchone()[0]

        select_exprs = []
        for data_source, source_col, output_col in self.column_mappings:
            if source_col != output_col:
                select_exprs.append(f'{data_source}."{source_col}" AS "{output_col}"')
            else:
                select_exprs.append(f'{data_source}."{source_col}"')

        join_clauses = []
        for data_source in sources:
            if data_source != self.seed_data_source:
                join_clauses.append(f'LEFT JOIN {data_source} ON {self.seed_data_source}."{self.join_key}" = {data_source}."{self.join_key}"')

        query = f'SELECT {", ".join(select_exprs)} FROM {self.seed_data_source} {" ".join(join_clauses)}'
        return self.duckdb_conn.sql(query)

    def _validate_duckdb(self, data: Any) -> None:
        expected = {field.name for field in IntegratedMetaModel._pa_silver_schema}
        actual = set(data.columns)
        missing = sorted(expected - actual)
        if missing:
            raise ValueError(f'Missing columns for integrated schema contract: {missing}')
        extra = sorted(actual - expected)
        if extra:
            raise ValueError(f'Extra columns not in model contract (will be kept): {extra}')

    def _write_duckdb(self, data: Any) -> None:
        import pyarrow as pa

        from data_integration_pipeline.common.io.delta_client import DeltaClient

        delta_client = DeltaClient()
        reader = data.fetch_record_batch(DEFAULT_CHUNK_SIZE)
        for batch in reader:
            delta_client.write(
                table_name=self.metadata.output_delta_table,
                data=pa.Table.from_batches([batch]),
                primary_key=self.primary_key,
                partition_key=self.partition_key,
            )
        self.metadata.n_output_records = delta_client.get_count(self.metadata.output_delta_table)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _read_sources(self) -> dict:
        if self.metadata.backend_engine == 'spark':
            return self._read_sources_spark()
        return self._read_sources_duckdb()

    def _join(self, sources: dict) -> Any:
        if self.metadata.backend_engine == 'spark':
            return self._join_spark(sources)
        return self._join_duckdb(sources)

    def _validate(self, data: Any) -> None:
        if self.metadata.backend_engine == 'spark':
            self._validate_spark(data)
        else:
            self._validate_duckdb(data)
        logger.info('Schema contract validation passed')

    def _write(self, data: Any) -> None:
        if self.metadata.backend_engine == 'spark':
            self._write_spark(data)
        else:
            self._write_duckdb(data)

    def _cleanup(self) -> None:
        if self.metadata.backend_engine == 'spark':
            self.spark_client.stop()
        else:
            self.duckdb_conn.close()

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
            self._cleanup()
        return self.metadata
