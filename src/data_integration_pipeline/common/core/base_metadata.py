from datetime import datetime
from typing import ClassVar, Optional

from data_integration_pipeline.common.io.logger import logger
from pydantic import BaseModel, Field

from data_integration_pipeline.common.core.utils import get_run_id, get_timestamp
from data_integration_pipeline.common.io.cloud_storage import CloudStorageClient
from data_integration_pipeline.settings import __VERSION__, SERVICE_NAME


class BaseMetadata(BaseModel):
    service_name: ClassVar[str] = SERVICE_NAME
    service_version: ClassVar[str] = __VERSION__
    run_id: str = Field(default_factory=get_run_id)
    start_timestamp: Optional[datetime] = Field(default_factory=get_timestamp)
    end_timestamp: Optional[datetime] = Field(default=None)

    def save(self, s3_path: Optional[str] = None) -> str:
        if s3_path is None:
            s3_path = self.metadata_s3_path
        client = CloudStorageClient()
        client.write_json(s3_path=s3_path, data=self.model_dump_json())
        logger.info(f'Wrote metadata to {s3_path}')
        return s3_path

    @staticmethod
    def format_list(title, items):
        lines = [f'{title}:']
        for item in items:
            lines.append(f'\t• {item}')
        return '\n'.join(lines)

    def complete(self, s3_path: Optional[str] = None):
        self.end_timestamp = get_timestamp()
        self.save(s3_path=s3_path)
