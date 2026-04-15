from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import (
    AliasPath,
    Field,
)

from data_integration_pipeline.common.core.models.templates.base_models import BaseSilverSchemaRecord
from data_integration_pipeline.common.core.models.utils.pydantic_utils import SoftNonNegativeInt


class SilverSchemaRecord(BaseSilverSchemaRecord):
    _primary_key: ClassVar[str] = 'id__company'
    _partition_key: ClassVar[str] = 'loc__iso2_country_code'

    id__company: str = Field(validation_alias=AliasPath('identifiers', 'company'))

    # --- LOCATION ---
    loc__country: Optional[str] = Field(default=None, validation_alias=AliasPath('location', 'country'))
    loc__iso2_country_code: Optional[str] = Field(default=None, validation_alias=AliasPath('location', 'iso2_country_code'))

    # --- URLS ---
    url__request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('urls', 'request_url'))
    url__normalized_request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('urls', 'normalized_request_url'))

    # --- COUNTS ---
    cnt__employees: Optional[int] = Field(default=None, validation_alias=AliasPath('counts', 'employees_count'))

    # --- DESCRIPTIONS ---
    desc__industry: Optional[str] = Field(default=None, validation_alias=AliasPath('descriptions', 'industry'))
    desc__description_short: Optional[str] = Field(default=None, validation_alias=AliasPath('descriptions', 'description_short'))

    # --- DATES ---
    dt__founded_year: Optional[SoftNonNegativeInt] = Field(default=None, validation_alias=AliasPath('dates', 'founded_year'))

    # --- FINANCIALS ---
    fin__revenue_quantity_lower_bound: Optional[SoftNonNegativeInt] = Field(
        default=None, validation_alias=AliasPath('financials', 'revenue_quantity_lower_bound')
    )
    fin__revenue_quantity_upper_bound: Optional[SoftNonNegativeInt] = Field(
        default=None, validation_alias=AliasPath('financials', 'revenue_quantity_upper_bound')
    )
    fin__revenue_unit_lower_bound: Optional[str] = Field(default=None, validation_alias=AliasPath('financials', 'revenue_unit_lower_bound'))
    fin__revenue_unit_upper_bound: Optional[str] = Field(default=None, validation_alias=AliasPath('financials', 'revenue_unit_upper_bound'))


if __name__ == '__main__':
    record = {
        'identifiers': {'company': 'cmp_000001'},
        'location': {'country': 'Finland', 'iso2_country_code': 'FI'},
        'counts': {'employees_count': 240},
        'descriptions': {'industry': 'Fintech', 'description_short': None},
        'dates': {'founded_year': 2018},
        'financials': {
            'revenue_quantity_lower_bound': 100,
            'revenue_quantity_upper_bound': 500,
            'revenue_unit_lower_bound': 'M',
            'revenue_unit_upper_bound': 'M',
        },
        'urls': {'request_url': 'https://www.nordic-fintech-solutions.ai/', 'normalized_request_url': 'nordic-fintech-solutions.ai'},
    }
    record = SilverSchemaRecord(**record)
    print(record)
    print(record.model_dump())
