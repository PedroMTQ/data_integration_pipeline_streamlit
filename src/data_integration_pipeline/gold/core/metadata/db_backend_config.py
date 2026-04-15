from __future__ import annotations

from typing import Annotated, ClassVar, Literal, Optional

from pydantic import BaseModel, Field, SerializationInfo, SerializerFunctionWrapHandler, model_serializer

from data_integration_pipeline.settings import (
    POSTGRES_CLIENT_BATCH_SIZE,
    POSTGRES_MAX_RETRIES,
    POSTGRES_RETRY_BACKOFF_SECONDS,
    POSTGRES_RETRY_MAX_BACKOFF_SECONDS,
    ES_CLIENT_BATCH_SIZE,
    ES_CLIENT_RAISE_ERRORS,
)
from data_integration_pipeline.gold.schemas.es_integrated_gold import _INTEGRATED_GOLD_INDEX_TEMPLATE

CONFIDENTIAL_MARKER = 'confidential'
CONFIDENTIAL_FIELD = Annotated[Optional[str], CONFIDENTIAL_MARKER]
NUMBER_OF_SHARDS = _INTEGRATED_GOLD_INDEX_TEMPLATE['settings']['number_of_shards']


class BaseBackendConfig(BaseModel):
    @staticmethod
    def _is_confidential_field(field_info) -> bool:
        return CONFIDENTIAL_MARKER in field_info.metadata

    @model_serializer(mode='wrap')
    def serialize_model(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        model_dump = handler(self)
        if info.mode == 'json':
            for field_name, field_info in self.__class__.model_fields.items():
                if field_name in model_dump and self._is_confidential_field(field_info):
                    model_dump.pop(field_name)
        return model_dump


class PostgresBackendConfig(BaseBackendConfig):
    db_backend: ClassVar[Literal['postgres']] = 'postgres'
    host: str
    port: int
    user: str
    password: CONFIDENTIAL_FIELD = Field(default=None, repr=False)
    database: str
    batch_size: int = POSTGRES_CLIENT_BATCH_SIZE

    def __str__(self) -> str:
        return f'\n\tHost:\t{self.host}\n\tPort:\t{self.port}\n\tUser:\t{self.user}\n\tDatabase:\t{self.database}\n\tBatch size:\t{self.batch_size}'


class ElasticsearchBackendConfig(BaseBackendConfig):
    """Mirrors ``ElasticsearchClient`` constructor so ``model_dump()`` can be passed through."""

    db_backend: ClassVar[Literal['elasticsearch']] = 'elasticsearch'
    urls: list[str]
    user: Optional[str] = None
    password: CONFIDENTIAL_FIELD = Field(default=None, repr=False)
    index_name: str
    index_alias: str
    number_of_shards: int = NUMBER_OF_SHARDS
    batch_size: int = Field(default=ES_CLIENT_BATCH_SIZE)
    raise_errors: bool = Field(default=ES_CLIENT_RAISE_ERRORS)
    max_retries: int = Field(default=POSTGRES_MAX_RETRIES)
    retry_backoff_seconds: float = Field(default=POSTGRES_RETRY_BACKOFF_SECONDS)
    retry_max_backoff_seconds: float = Field(default=POSTGRES_RETRY_MAX_BACKOFF_SECONDS)

    def __str__(self) -> str:
        return '\n'.join(
            [
                f'\tUrls:\t{self.urls}',
                f'\tUser:\t{self.user}',
                f'\tIndex name:\t{self.index_name}',
                f'\tIndex alias:\t{self.index_alias}',
                f'\tBatch size:\t{self.batch_size}',
                f'\tNumber of shards:\t{self.number_of_shards}',
                f'\tRaise errors:\t{self.raise_errors}',
                f'\tMax retries:\t{self.max_retries}',
                f'\tRetry backoff (s):\t{self.retry_backoff_seconds}',
                f'\tRetry max backoff (s):\t{self.retry_max_backoff_seconds}',
            ]
        )


if __name__ == '__main__':
    config = PostgresBackendConfig(host='localhost', port=5432, user='postgres', password='postgres', database='test')
    # print(config)
    print(config.model_dump())
    print(config.model_dump_json())
