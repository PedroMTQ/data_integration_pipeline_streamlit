from contextlib import closing
from typing import Any, Iterable

from pydantic import ValidationError
from retry import retry

from data_integration_pipeline.common.core.metrics import log_metrics
from data_integration_pipeline.common.core.models.integrated.metamodel import IntegratedMetaModel
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.common.core.timed_trigger import TimedTrigger
from data_integration_pipeline.common.io.delta_client import DeltaClient
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.gold.core.metadata.sync_base_metadata import sync_last_synced_item, LastSyncedItem
from data_integration_pipeline.gold.core.metadata.sync_integrated_postgres_metadata import SyncIntegratedPostgresMetadata
from data_integration_pipeline.gold.io import get_db_client
from data_integration_pipeline.gold.io.postgres_client import PostgresClient
from data_integration_pipeline.gold.io.schema_manager import SchemaManager
from data_integration_pipeline.settings import (
    DELTA_CLIENT_WRITE_RETRIES,
    DELTA_CLIENT_WRITE_RETRY_BACKOFF,
    DELTA_CLIENT_WRITE_RETRY_DELAY,
    DELTA_CLIENT_WRITE_RETRY_JITTER,
    DELTA_CLIENT_WRITE_RETRY_MAX_DELAY,
    HASH_DIFF_COLUMN,
    LOG_DELAY,
    POSTGRES_MAX_RETRIES,
    POSTGRES_RETRY_BACKOFF_SECONDS,
    POSTGRES_RETRY_DELAY,
    POSTGRES_RETRY_JITTER,
    POSTGRES_RETRY_MAX_BACKOFF_SECONDS,
    SYNC_METADATA_FREQUENCY,
)


class SyncIntegratedPostgresProcessor:
    """Streams integrated silver Delta, maps to gold, upserts into Postgres."""

    def __init__(self, metadata: SyncIntegratedPostgresMetadata):
        self.data_model = ModelMapper.get_data_model(metadata.input_delta_table)
        if not self.data_model:
            raise ValueError(f'No data model found for {metadata.input_delta_table}')
        self.metadata = metadata
        self.db_batch_size = metadata.output_db_backend_config.batch_size
        self.delta_client = DeltaClient()
        self.postgres_client: PostgresClient = get_db_client(metadata.output_db_backend_config)
        self.postgres_client.ping()
        self.log_timed_trigger = TimedTrigger(delay=LOG_DELAY, function=log_metrics)
        self.sync_timed_trigger = TimedTrigger(delay=SYNC_METADATA_FREQUENCY, function=sync_last_synced_item)
        self.gold_primary_key = ModelMapper.get_primary_key(self.metadata.output_postgres_table, data_stage='gold')

    def _ensure_schema(self) -> None:
        schema_manager = SchemaManager(db_client=self.postgres_client)
        schema_manager.run()

    def _validate_resume(self) -> None:
        """Check that the stored LDTS matches the current table; reset if stale."""
        current_ldts = self.delta_client.get_last_commit_timestamp(self.metadata.input_delta_table)
        self._current_ldts = current_ldts if current_ldts else None
        if self.metadata.last_synced_item is not None:
            stored_ldts = self.metadata.last_synced_item.ldts
            if stored_ldts != self._current_ldts:
                logger.warning(f'Delta table LDTS changed ({stored_ldts} -> {self._current_ldts}), resetting resume state')
                self.metadata.last_synced_item = None

    def _get_resume_fragment(self) -> int:
        if self.metadata.last_synced_item is not None:
            return self.metadata.last_synced_item.item
        return -1

    def _update_fragment(self, fragment_index: int) -> None:
        """Update the in-memory resume state. Persistence is handled by ``sync_timed_trigger``."""
        self.metadata.last_synced_item = LastSyncedItem(item=fragment_index, ldts=self._current_ldts)

    @retry(
        tries=DELTA_CLIENT_WRITE_RETRIES,
        delay=DELTA_CLIENT_WRITE_RETRY_DELAY,
        backoff=DELTA_CLIENT_WRITE_RETRY_BACKOFF,
        max_delay=DELTA_CLIENT_WRITE_RETRY_MAX_DELAY,
        jitter=DELTA_CLIENT_WRITE_RETRY_JITTER,
    )
    def yield_data(self) -> Iterable[dict]:
        """Stream gold records from Delta, skipping already-processed fragments."""
        resume_fragment = self._get_resume_fragment()
        if resume_fragment >= 0:
            logger.info(f'Resuming Delta read: skipping fragments <= {resume_fragment}')
        try:
            for fragment_index, batches in self.delta_client.read_fragments(
                table_name=self.metadata.input_delta_table, min_fragment_index=resume_fragment
            ):
                with closing(batches) as stream:
                    for batch in stream:
                        for row in batch.to_pylist():
                            self.log_timed_trigger.trigger(metrics=self.metadata.metrics)
                            self.sync_timed_trigger.trigger(metadata=self.metadata, last_synced_item=self.metadata.last_synced_item)
                            # TODO add error dumping to S3
                            try:
                                record = IntegratedMetaModel.from_silver_record(row)
                                gold_record = record.gold_record
                                if gold_record is None:
                                    self.metadata.metrics.log_result(is_success=False)
                                    continue
                                self.metadata.metrics.log_result(is_success=True)
                                record_dict = gold_record.model_dump()
                                yield record_dict
                            except (ValidationError, Exception) as e:
                                self.metadata.metrics.log_result(is_success=False)
                                logger.warning(f'Skipping invalid integrated silver row: {e}')
                self._update_fragment(fragment_index)
        except (Exception, KeyboardInterrupt):
            sync_last_synced_item(metadata=self.metadata, last_synced_item=self.metadata.last_synced_item)
            raise

    @retry(
        tries=POSTGRES_MAX_RETRIES,
        delay=POSTGRES_RETRY_DELAY,
        backoff=POSTGRES_RETRY_BACKOFF_SECONDS,
        max_delay=POSTGRES_RETRY_MAX_BACKOFF_SECONDS,
        jitter=POSTGRES_RETRY_JITTER,
    )
    def load_data(self) -> None:
        """Upsert yielded rows into Postgres; fresh ``yield_data()`` on each retry attempt."""
        if self.metadata.last_synced_item is not None:
            logger.info(f'Resuming Postgres sync from fragment > {self._get_resume_fragment()}')
        buffer: list[dict[str, Any]] = []
        try:
            with closing(self.yield_data()) as stream:
                for record in stream:
                    buffer.append(record)
                    if len(buffer) >= self.db_batch_size:
                        self._flush(buffer)
                        buffer = []
                if buffer:
                    self._flush(buffer)
        except (Exception, KeyboardInterrupt):
            sync_last_synced_item(metadata=self.metadata, last_synced_item=self.metadata.last_synced_item)
            raise

    def _flush(self, buffer: list[dict[str, Any]]) -> None:
        count = self.postgres_client.upsert(
            table_name=self.metadata.output_postgres_table,
            records=buffer,
            upsert_key=self.gold_primary_key,
            diff_key=HASH_DIFF_COLUMN,
            exclude_from_update={self.gold_primary_key},
        )
        self.metadata.n_output_records += count

    def run(self) -> SyncIntegratedPostgresMetadata:
        self._ensure_schema()
        self._validate_resume()
        logger.info(f'Reading and upserting data from {self.metadata.input_delta_table} to {self.metadata.output_postgres_table}')
        self.load_data()
        self.metadata.sync_finished = True
        return self.metadata
