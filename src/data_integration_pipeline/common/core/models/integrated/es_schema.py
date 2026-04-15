from __future__ import annotations
from typing import Any, Optional

from pydantic import ConfigDict, Field
from data_integration_pipeline.common.core.models.templates.base_models import BaseElasticsearchSchemaRecord


class ElasticsearchSchemaRecord(BaseElasticsearchSchemaRecord):
    """ES document shape -- only search-relevant fields from ``integrated_records``.

    PG remains the serving layer for full record details; ES is the search index.
    Field names match ``integrated_records`` columns directly (no aliases).
    """

    model_config = ConfigDict(populate_by_name=True)

    # --- identifiers ---
    hk: str
    company_id: str

    # --- keyword / filter ---
    country: Optional[str] = Field(default=None)
    iso2_country_code: Optional[str] = Field(default=None)
    normalized_request_url: Optional[str] = Field(default=None, alias='url__normalized_request_url')

    # --- text search ---
    industry: Optional[str] = Field(default=None)
    description_short: Optional[str] = Field(default=None)
    description_long: Optional[str] = Field(default=None)

    # --- arrays ---
    product_services: Optional[list[str]] = Field(default=None)
    market_niches: Optional[list[str]] = Field(default=None)
    business_model: Optional[list[str]] = Field(default=None)

    # --- faceted / numeric ---
    employees_count: Optional[int] = Field(default=None)
    founded_year: Optional[int] = Field(default=None)
    revenue_quantity_lower_bound: Optional[int] = Field(default=None)
    revenue_quantity_upper_bound: Optional[int] = Field(default=None)

    # --- dense vectors ---
    embedding_global_vector: Optional[list[float]] = Field(default=None, repr=False)
    embedding_description_long: Optional[list[float]] = Field(default=None, repr=False)
    embedding_description_short: Optional[list[float]] = Field(default=None, repr=False)
    embedding_product_services: Optional[list[float]] = Field(default=None, repr=False)
    embedding_market_niches: Optional[list[float]] = Field(default=None, repr=False)
    embedding_business_model: Optional[list[float]] = Field(default=None, repr=False)

    @classmethod
    def from_postgres_row(cls, row: dict[str, Any]) -> 'ElasticsearchSchemaRecord':
        """Build from an ``integrated_records`` row dict. Extra keys are ignored."""
        row['hk'] = str(row['hk'])
        return cls(**{k: v for k, v in row.items() if k in cls.model_fields})
