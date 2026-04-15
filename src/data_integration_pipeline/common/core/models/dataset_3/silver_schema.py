from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Optional

from pydantic import AliasPath, Field

from data_integration_pipeline.common.core.models.templates.base_models import BaseSilverSchemaRecord
from data_integration_pipeline.common.core.models.templates.base_model_embedding import BaseModelEmbedding
from data_integration_pipeline.settings import EMBEDDING_LDTS_COLUMN


class SilverSchemaRecord(BaseSilverSchemaRecord):
    _primary_key: ClassVar[str] = 'url__normalized_request_url'
    _partition_key: ClassVar[str] = 'url__partition_key_url'

    # --- URLS ---
    url__request_url: str = Field(validation_alias=AliasPath('urls', 'request_url'))
    url__normalized_request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('urls', 'normalized_request_url'))
    url__partition_key_url: Optional[str] = Field(default=None, validation_alias=AliasPath('urls', 'partition_key_url'))

    # --- EMBEDDINGS ---
    emb__global_vector: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('embeddings', 'global_vector'))
    emb__description_long: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('embeddings', 'description_long'))
    emb__description_short: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('embeddings', 'description_short'))
    emb__product_services: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('embeddings', 'product_services'))
    emb__market_niches: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('embeddings', 'market_niches'))
    emb__business_model: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('embeddings', 'business_model'))
    # --- DATES ---
    dt__embedding_ldts: Optional[datetime] = Field(default=None, validation_alias=AliasPath('dates', EMBEDDING_LDTS_COLUMN), repr=False)


if __name__ == '__main__':
    from data_integration_pipeline.common.core.models.dataset_3.bronze_schema import BronzeSchemaRecord

    doc = {
        'url': 'https://www.nordic-fintech-solutions.ai',
        'global_vector': [
            -0.3907,
        ],
        'business_description_emb': [
            -0.3907,
        ],
        'product_services_emb': [
            -0.3907,
        ],
        'market_niches_emb': [
            -0.3907,
        ],
        'business_model_emb': [
            -0.3907,
        ],
        'short_description_emb': [
            -0.3907,
        ],
        'embedding_updated_at': '2024-10-21T06:52:31Z',
    }
    bronze = BronzeSchemaRecord(**doc)
    record = SilverSchemaRecord(**bronze.model_dump())
    print(record)
    print(record.model_dump())
