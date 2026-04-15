from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import Field, computed_field, model_validator, field_validator

from data_integration_pipeline.common.core.models.templates.base_models import BaseInnerModel, BaseBronzeSchemaRecord
from data_integration_pipeline.common.core.models.templates.base_model_embedding import BaseModelEmbedding
from data_integration_pipeline.common.core.models.templates.base_model_url import BaseModelURL
from data_integration_pipeline.common.core.models.utils.pydantic_utils import SoftStr


class ModelURLs(BaseModelURL):
    request_url: SoftStr = Field(default=None, alias='url', description='Website address')

    @computed_field(description='Partition key for the website URL')
    def partition_key_url(self) -> Optional[str]:
        return BaseModelURL.get_partition_key(self.normalized_request_url)


class ModelEmbeddings(BaseInnerModel):
    global_vector: Optional[BaseModelEmbedding] = Field(
        default=None,
        alias='global_emb',
        description='Embedding vector for global text (includes descriptions and keywords)',
    )
    description_long: Optional[BaseModelEmbedding] = Field(
        default=None,
        alias='business_description_emb',
        description='Embedding vector for business description',
    )
    description_short: Optional[BaseModelEmbedding] = Field(
        default=None,
        alias='short_description_emb',
        description='Embedding vector for short description',
    )
    product_services: Optional[BaseModelEmbedding] = Field(
        default=None,
        alias='product_services_emb',
        description='Embedding vector for products and services',
    )
    market_niches: Optional[BaseModelEmbedding] = Field(
        default=None,
        alias='market_niches_emb',
        description='Embedding vector for market niches',
    )
    business_model: Optional[BaseModelEmbedding] = Field(
        default=None,
        alias='business_model_emb',
        description='Embedding vector for business model',
    )

    @field_validator('global_vector', 'description_long', 'description_short', 'product_services', 'market_niches', 'business_model', mode='after')
    @classmethod
    def remove_null_values(cls, v) -> Optional[BaseModelEmbedding]:
        if v is None:
            return None
        if v.embedding is None:
            return None
        return v


class ModelDates(BaseInnerModel):
    embedding_ldts: Optional[datetime] = Field(
        default=None,
        alias='embedding_updated_at',
        description='Timestamp when embedding fields were last updated',
    )


class BronzeSchemaRecord(BaseBronzeSchemaRecord):
    urls: ModelURLs
    embeddings: ModelEmbeddings
    dates: ModelDates

    @model_validator(mode='before')
    @classmethod
    def distribute_flat_data(cls, data: Any) -> Any:
        """
        Takes the flat input dictionary and assigns the exact same dictionary
        to every field. The sub-models will filter what they need
        because they have extra='ignore'.
        """
        data = {k: v if v != '' else None for k, v in data.items()}
        if isinstance(data, dict):
            return {
                'urls': data,
                'embeddings': data,
                'dates': data,
            }
        return data


if __name__ == '__main__':
    doc = {
        'url': 'https://www.nordic-fintech-solutions.ai',
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
    record = BronzeSchemaRecord(**doc)
    print(record.model_dump())
