from contextlib import closing
from typing import Any, Generator, Iterable

from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.common.core.timed_trigger import TimedTrigger
from retry import retry

from data_integration_pipeline.common.core.metrics import log_metrics
from data_integration_pipeline.common.core.models.integrated.es_schema import ElasticsearchSchemaRecord
from data_integration_pipeline.gold.core.metadata.sync_base_metadata import LastSyncedItem, sync_last_synced_item
from data_integration_pipeline.gold.core.metadata.sync_elastic_search_metadata import SyncElasticSearchMetadata
from data_integration_pipeline.gold.io import get_db_client
from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
from data_integration_pipeline.settings import (
    POSTGRES_MAX_RETRIES,
    POSTGRES_RETRY_BACKOFF_SECONDS,
    POSTGRES_RETRY_DELAY,
    POSTGRES_RETRY_JITTER,
    POSTGRES_RETRY_MAX_BACKOFF_SECONDS,
    SYNC_METADATA_FREQUENCY,
    LOG_DELAY,
)
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper


class SyncElasticSearchProcessor:
    def __init__(self, metadata: SyncElasticSearchMetadata):
        self.metadata = metadata
        self.db_batch_size = metadata.input_db_backend_config.batch_size
        for db_backend in [self.metadata.input_db_backend_config, self.metadata.output_db_backend_config]:
            try:
                get_db_client(db_backend).ping()
            except Exception as e:
                logger.critical(f'Failed to connect to {db_backend}')
                raise e
        self.log_timed_trigger = TimedTrigger(delay=LOG_DELAY, function=log_metrics)
        self.sync_timed_trigger = TimedTrigger(delay=SYNC_METADATA_FREQUENCY, function=sync_last_synced_item)
        self.gold_primary_key = ModelMapper.get_primary_key(self.metadata.input_postgres_table, data_stage='gold')

    def _get_resume_pk(self) -> str | None:
        if self.metadata.last_synced_item is not None:
            return self.metadata.last_synced_item.item
        return None

    def _build_select_query(self) -> tuple[str, dict]:
        sql = f'SELECT * FROM {self.metadata.input_postgres_table}'
        params: dict[str, Any] = {}
        resume_pk = self._get_resume_pk()
        if resume_pk is not None:
            sql += f' WHERE {self.gold_primary_key} > %(last_synced_pk)s'
            params['last_synced_pk'] = resume_pk
        sql += f' ORDER BY {self.gold_primary_key}'
        return sql, params

    @retry(
        tries=POSTGRES_MAX_RETRIES,
        delay=POSTGRES_RETRY_DELAY,
        backoff=POSTGRES_RETRY_BACKOFF_SECONDS,
        max_delay=POSTGRES_RETRY_MAX_BACKOFF_SECONDS,
        jitter=POSTGRES_RETRY_JITTER,
    )
    def yield_data(self) -> Iterable[dict]:
        db_client = get_db_client(self.metadata.input_db_backend_config)
        sql, params = self._build_select_query()
        with db_client.db_connection.cursor() as cursor:
            with closing(cursor.stream(sql, params=params)) as stream:
                try:
                    column_names = None
                    for row_tuple in stream:
                        if column_names is None:
                            column_names = [desc[0] for desc in cursor.description]
                        self.log_timed_trigger.trigger(metrics=self.metadata.metrics)
                        self.sync_timed_trigger.trigger(metadata=self.metadata, last_synced_item=self.metadata.last_synced_item)
                        row_dict = dict(zip(column_names, row_tuple, strict=True))
                        doc = ElasticsearchSchemaRecord.from_postgres_row(row_dict)
                        if not doc:
                            continue
                        record_dict = doc.model_dump()
                        yield record_dict
                except (Exception, KeyboardInterrupt):
                    sync_last_synced_item(metadata=self.metadata, last_synced_item=self.metadata.last_synced_item)
                    raise

    @staticmethod
    def _es_get_record_id(item: dict) -> str | None:
        """Document id from a single ``streaming_bulk`` result item (``{op: {...}}``)."""
        for op in ('index', 'create', 'update', 'delete'):
            inner = item.get(op)
            if isinstance(inner, dict) and inner.get('_id') is not None:
                return inner.get('_id')
        return None

    @retry(
        tries=POSTGRES_MAX_RETRIES,
        delay=POSTGRES_RETRY_DELAY,
        backoff=POSTGRES_RETRY_BACKOFF_SECONDS,
        max_delay=POSTGRES_RETRY_MAX_BACKOFF_SECONDS,
        jitter=POSTGRES_RETRY_JITTER,
    )
    def load_data(self) -> None:
        resume_pk = self._get_resume_pk()
        if resume_pk is not None:
            logger.info(f'Resuming ES sync from hk > {resume_pk}')
        es_client: ElasticsearchClient = get_db_client(self.metadata.output_db_backend_config)
        data_stream = self.yield_data()
        with closing[Generator[tuple, Any, None]](es_client.load_stream(data_stream=data_stream)) as stream:
            try:
                for is_success, item in stream:
                    if is_success:
                        record_id = self._es_get_record_id(item)
                        if record_id is None:
                            continue
                        self.metadata.last_synced_item = LastSyncedItem(item=record_id)
                        self.metadata.metrics.log_result(is_success=True)
                    else:
                        self.metadata.metrics.log_result(is_success=False)
                        raise RuntimeError(f'Elasticsearch bulk item failed: {item}')
            except (Exception, KeyboardInterrupt):
                sync_last_synced_item(metadata=self.metadata, last_synced_item=self.metadata.last_synced_item)
                raise

    def run(self) -> SyncElasticSearchMetadata:
        es_client: ElasticsearchClient = get_db_client(self.metadata.output_db_backend_config)
        es_client.init_index()
        self.load_data()
        self.metadata.sync_finished = True
        return self.metadata
