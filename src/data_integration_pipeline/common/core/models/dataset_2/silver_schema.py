from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import AliasPath, Field

from data_integration_pipeline.common.core.models.templates.base_models import BaseSilverSchemaRecord


class SilverSchemaRecord(BaseSilverSchemaRecord):
    _primary_key: ClassVar[str] = 'url__normalized_request_url'
    _partition_key: ClassVar[str] = 'url__partition_key_url'

    # --- URLS ---
    url__request_url: str = Field(validation_alias=AliasPath('urls', 'request_url'))
    url__normalized_request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('urls', 'normalized_request_url'))
    url__partition_key_url: Optional[str] = Field(default=None, validation_alias=AliasPath('urls', 'partition_key_url'))

    # --- DESCRIPTIONS ---
    desc__description_long: Optional[str] = Field(default=None, validation_alias=AliasPath('descriptions', 'description_long'))
    desc__product_services: Optional[list[str]] = Field(default=None, validation_alias=AliasPath('descriptions', 'product_services'))
    desc__market_niches: Optional[list[str]] = Field(default=None, validation_alias=AliasPath('descriptions', 'market_niches'))
    desc__business_model: Optional[list[str]] = Field(default=None, validation_alias=AliasPath('descriptions', 'business_model'))


if __name__ == '__main__':
    from data_integration_pipeline.common.core.models.dataset_2.bronze_schema import BronzeSchemaRecord

    doc = {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection, risk analytics, and digital banking workflows for financial institutions.',
        'product_services': 'AI underwriting\nFraud detection platform\nRisk monitoring\nBanking analytics dashboards',
        'market_niches': 'Digital Banking\nPayments\nFinancial Infrastructure\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }
    bronze = BronzeSchemaRecord(**doc)
    record = SilverSchemaRecord(**bronze.model_dump())
    print(record)
    print(record.model_dump())
