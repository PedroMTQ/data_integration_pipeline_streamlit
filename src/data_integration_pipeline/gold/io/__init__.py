from typing import Union

from data_integration_pipeline.gold.core.metadata.db_backend_config import ElasticsearchBackendConfig, PostgresBackendConfig
from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
from data_integration_pipeline.gold.io.postgres_client import PostgresClient


def get_db_client(
    backend_config: Union[PostgresBackendConfig, ElasticsearchBackendConfig],
) -> Union[PostgresClient, ElasticsearchClient]:
    if isinstance(backend_config, PostgresBackendConfig):
        return PostgresClient(**backend_config.model_dump(exclude_none=True))
    elif isinstance(backend_config, ElasticsearchBackendConfig):
        return ElasticsearchClient(**backend_config.model_dump(exclude_none=True, exclude={'number_of_shards'}))
    else:
        raise ValueError(f'Invalid database backend config: {backend_config}')
