from __future__ import annotations

from typing import Any, Optional

from pydantic import Field, computed_field, model_validator, field_validator

from data_integration_pipeline.common.core.models.templates.base_models import BaseInnerModel, BaseBronzeSchemaRecord
from data_integration_pipeline.common.core.models.utils.pydantic_utils import SoftStr
from data_integration_pipeline.common.core.models.templates.base_model_url import BaseModelURL


class ModelURLs(BaseModelURL):
    request_url: SoftStr = Field(default=None, alias='url', description='Website address')

    @computed_field(description='Partition key for the website URL')
    def partition_key_url(self) -> Optional[str]:
        return BaseModelURL.get_partition_key(self.normalized_request_url)


class ModelDescriptions(BaseInnerModel):
    description_long: SoftStr = Field(
        default=None,
        alias='business_description',
        description='Narrative description of what the company does',
    )
    product_services: Optional[list[SoftStr]] = Field(
        default=None,
        alias='product_services',
        description='Products and services offered',
    )
    market_niches: Optional[list[SoftStr]] = Field(
        default=None,
        alias='market_niches',
        description='Market segments and niches',
    )
    business_model: Optional[list[SoftStr]] = Field(
        default=None,
        alias='business_model',
        description='Business model summary',
    )

    def split_value(value: str, split_char: str) -> list[str]:
        if isinstance(value, list):
            descriptions = [i.strip() for i in value]
            return [i for i in descriptions if i]
        if isinstance(value, str):
            descriptions = [i.strip() for i in value.split(split_char)]
            return [i for i in descriptions if i]
        return []

    @field_validator('product_services', 'market_niches', mode='before')
    @classmethod
    def split_keywords(cls, v) -> list[str]:
        return cls.split_value(v, '\n')

    @field_validator('business_model', mode='before')
    @classmethod
    def split_business_model(cls, v) -> list[str]:
        return cls.split_value(v, '|')

    @field_validator('product_services', 'market_niches', 'business_model', mode='after')
    @classmethod
    def remove_null_values(cls, v) -> list[str]:
        return [i for i in v if i is not None]


class BronzeSchemaRecord(BaseBronzeSchemaRecord):
    urls: ModelURLs
    descriptions: ModelDescriptions

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
                'descriptions': data,
            }
        return data


if __name__ == '__main__':
    doc = {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection, risk analytics, and digital banking workflows for financial institutions.',
        'product_services': 'AI underwriting\nFraud detection platform\nRisk monitoring\nBanking analytics dashboards',
        'market_niches': 'Digital Banking\nPayments\nFinancial Infrastructure\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }
    record = BronzeSchemaRecord(**doc)
    print(record.model_dump(mode='data'))
