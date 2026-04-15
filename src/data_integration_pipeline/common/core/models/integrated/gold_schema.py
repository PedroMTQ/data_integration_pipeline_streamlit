from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional, ClassVar

from pydantic import Field, computed_field, AliasPath

from data_integration_pipeline.common.core.models.templates.base_models import BaseGoldSchemaRecord
from data_integration_pipeline.common.core.utils import get_hash_for_payload, get_hk
from data_integration_pipeline.settings import HASH_DIFF_COLUMN, LOAD_LDTS_COLUMN, SYNC_LDTS_COLUMN, EMBEDDING_LDTS_COLUMN

HASH_DIFF_EXCLUDED_FIELDS = {
    'company_id',
    'hk',
    'hdiff',
    LOAD_LDTS_COLUMN,
    SYNC_LDTS_COLUMN,
    HASH_DIFF_COLUMN,
}


class GoldSchemaRecord(BaseGoldSchemaRecord):
    """
    Gold row for the integrated dataset: descriptive names, each populated from
    the flat integrated silver record via ``Field(alias=<silver_field_name>)``.
    Silver fields marked ``exclude=True`` (alternate source URLs) are not mapped here.
    """

    _primary_key: ClassVar[str] = 'hk'

    # --- IDENTIFIERS (dataset 1)  ---
    company_id: str = Field(alias='id__company')

    # --- LOCATION (dataset 1)  ---
    country: Optional[str] = Field(default=None, alias='loc__country')
    iso2_country_code: Optional[str] = Field(default=None, alias='loc__iso2_country_code')

    # --- URLS (canonical / dataset 1) ---
    request_url: Optional[str] = Field(default=None, alias='url__request_url')
    normalized_request_url: Optional[str] = Field(default=None, alias='url__normalized_request_url')

    # --- COUNTS (dataset 1)  ---
    employees_count: Optional[int] = Field(default=None, alias='cnt__employees')

    # --- DESCRIPTIONS (dataset 1) ---
    industry: Optional[str] = Field(default=None, alias='desc__industry')
    description_short: Optional[str] = Field(default=None, alias='desc__description_short')

    founded_year: Optional[int] = Field(default=None, alias='dt__founded_year')

    # --- FINANCIALS (revenue band from registry) ---
    revenue_quantity_lower_bound: Optional[int] = Field(default=None, alias='fin__revenue_quantity_lower_bound')
    revenue_quantity_upper_bound: Optional[int] = Field(default=None, alias='fin__revenue_quantity_upper_bound')
    revenue_unit_lower_bound: Optional[str] = Field(default=None, alias='fin__revenue_unit_lower_bound')
    revenue_unit_upper_bound: Optional[str] = Field(default=None, alias='fin__revenue_unit_upper_bound')

    # --- ENRICHMENT TEXT (dataset 2) ---
    description_long: Optional[str] = Field(default=None, alias='desc__description_long')
    product_services: Optional[list[str]] = Field(default=None, alias='desc__product_services')
    market_niches: Optional[list[str]] = Field(default=None, alias='desc__market_niches')
    business_model: Optional[list[str]] = Field(default=None, alias='desc__business_model')

    # --- EMBEDDINGS (dataset 3) ---
    embedding_global_vector: Optional[list[float]] = Field(
        default=None, alias='emb__global_vector', validation_alias=AliasPath('emb__global_vector', 'embedding'), repr=False
    )
    embedding_description_long: Optional[list[float]] = Field(
        default=None, alias='emb__description_long', validation_alias=AliasPath('emb__description_long', 'embedding'), repr=False
    )
    embedding_description_short: Optional[list[float]] = Field(
        default=None, alias='emb__description_short', validation_alias=AliasPath('emb__description_short', 'embedding'), repr=False
    )
    embedding_product_services: Optional[list[float]] = Field(
        default=None, alias='emb__product_services', validation_alias=AliasPath('emb__product_services', 'embedding'), repr=False
    )
    embedding_market_niches: Optional[list[float]] = Field(
        default=None, alias='emb__market_niches', validation_alias=AliasPath('emb__market_niches', 'embedding'), repr=False
    )
    embedding_business_model: Optional[list[float]] = Field(
        default=None, alias='emb__business_model', validation_alias=AliasPath('emb__business_model', 'embedding'), repr=False
    )

    # comes from the dataset 3 data, can be None if there is no embedding
    embedding_ldts: Optional[datetime] = Field(default=None, validation_alias=AliasPath('dataset_3', f'dt__{EMBEDDING_LDTS_COLUMN}'), repr=False)

    @computed_field(description='Deterministic hub key from company id and integrated source name')
    def hk(self) -> uuid.UUID:
        return uuid.UUID(get_hk(source_id=self.company_id, source_name='integrated'))

    @computed_field(description='Hash of descriptive payload for change detection')
    def hdiff(self) -> str:
        payload = self.model_dump(by_alias=False, exclude=HASH_DIFF_EXCLUDED_FIELDS)
        return get_hash_for_payload(payload)
