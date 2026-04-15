from typing import Any, Optional

from data_integration_pipeline.common.io.logger import logger
from pydantic import Field, BaseModel

from data_integration_pipeline.common.core.base_metadata import BaseMetadata
from data_integration_pipeline.common.core.metrics import Metrics
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import METADATA_FILE_PATTERN
from datetime import datetime


class LastSyncedItem(BaseModel):
    item: Optional[Any] = None
    ldts: Optional[datetime] = None


class SyncBaseMetadata(BaseMetadata):
    n_input_records: Optional[int] = Field(default=0)
    n_output_records: Optional[int] = Field(default=0)
    metrics: Metrics = Field(default_factory=Metrics)
    last_synced_item: Optional[LastSyncedItem] = Field(default=None)
    sync_finished: bool = Field(default=False)

    @classmethod
    def _get_latest(cls, metadata_prefix: str) -> Optional[Any]:
        """Load the most recent metadata from S3 (by start_timestamp), including incomplete runs."""
        client = CloudStorageClient()
        metadata_files = client.get_files(prefix=metadata_prefix, regex_pattern=METADATA_FILE_PATTERN)
        if not metadata_files:
            return None
        latest: Optional[cls] = None
        for s3_path in metadata_files:
            try:
                entry = cls(**client.read_json(s3_path))
                if latest is None or entry.start_timestamp > latest.start_timestamp:
                    latest = entry
            except Exception as e:
                logger.warning(f'Failed to load metadata from {s3_path}: {e}')
        return latest


def sync_last_synced_item(metadata: SyncBaseMetadata, last_synced_item: Optional[LastSyncedItem]) -> None:
    metadata.last_synced_item = last_synced_item
    metadata.save()
