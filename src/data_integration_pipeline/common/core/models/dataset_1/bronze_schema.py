from __future__ import annotations

from typing import Any, Optional, ClassVar

from pydantic import (
    Field,
    model_validator,
)

from datetime import datetime
from data_integration_pipeline.common.core.models.templates.base_models import BaseInnerModel, BaseBronzeSchemaRecord
import re
from data_integration_pipeline.common.core.models.utils.data_utils import (
    standardize_country_information,
)
from data_integration_pipeline.common.core.models.utils.pydantic_utils import (
    SoftNonNegativeInt,
    SoftStr,
)
from data_integration_pipeline.common.core.models.templates.base_model_url import BaseModelURL


class ModelIdentifiers(BaseInnerModel):
    company: str = Field(alias='company_id', description='Unique identification number for this data source')


class ModelLocations(BaseInnerModel):
    country: SoftStr = Field(
        default=None,
        alias='country',
        description='Name of the country',
        min_length=3,
    )
    iso2_country_code: Optional[str] = Field(default=None, alias='CountryCode', description='ISO2 code of the country')

    @model_validator(mode='after')
    def validate_country_and_country_code(self):
        country_info = standardize_country_information(country_name=self.country)
        self.iso2_country_code = country_info.get('iso2_country_code')
        country = country_info.get('country_name')
        self.country = country if isinstance(country, str) else None
        return self


class ModelURLs(BaseModelURL):
    request_url: SoftStr = Field(default=None, alias='website_url', description='Website address')


class ModelCounts(BaseInnerModel):
    employees_count: Optional[SoftNonNegativeInt] = Field(
        default=None,
        alias='employee_count',
        description='The total number of employees in the business organization',
    )


class ModelDescriptions(BaseInnerModel):
    industry: SoftStr = Field(
        default=None,
        alias='industry',
        description='Industry of the business organization',
    )
    description_short: SoftStr = Field(
        default=None,
        alias='short_description',
        description='Short description of the business organization',
    )


# this could be useful for searchability and filtering
class ModelFinancials(BaseInnerModel):
    unit_search: ClassVar[re.Pattern] = re.compile(r'([a-zA-Z]+)')
    revenue_quantity_lower_bound: Optional[SoftNonNegativeInt] = Field(default=None, description='Lower bound of the revenue quantity')
    revenue_quantity_upper_bound: Optional[SoftNonNegativeInt] = Field(default=None, description='Upper bound of the revenue quantity')
    revenue_unit_lower_bound: Optional[str] = Field(default=None, description='Unit of the lower bound of the revenue')
    revenue_unit_upper_bound: Optional[str] = Field(default=None, description='Unit of the upper bound of the revenue')

    @model_validator(mode='before')
    @classmethod
    def extract_revenue_data(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        revenue_range = data.get('revenue_range')
        if not revenue_range or not isinstance(revenue_range, str) or '-' not in revenue_range:
            return {}
        parts = revenue_range.split('-', maxsplit=1)
        if len(parts) != 2:
            return {}
        lower_raw, upper_raw = parts[0].strip(), parts[1].strip()
        if not lower_raw or not upper_raw:
            return {}
        result: dict = {}
        lower_match = cls.unit_search.search(lower_raw)
        if lower_match:
            unit = lower_match.group(1)
            quantity = lower_raw.replace(unit, '').strip()
            if quantity.isdigit():
                result['revenue_quantity_lower_bound'] = int(quantity)
                result['revenue_unit_lower_bound'] = unit
        upper_match = cls.unit_search.search(upper_raw)
        if upper_match:
            unit = upper_match.group(1)
            quantity = upper_raw.replace(unit, '').strip()
            if quantity.isdigit():
                result['revenue_quantity_upper_bound'] = int(quantity)
                result['revenue_unit_upper_bound'] = unit
        return result


class ModelDates(BaseInnerModel):
    founded_year: Optional[SoftNonNegativeInt] = Field(
        default=None, alias='founded_year', description='Founding year of the business', ge=1000, le=datetime.now().year
    )


class BronzeSchemaRecord(BaseBronzeSchemaRecord):
    identifiers: ModelIdentifiers
    location: ModelLocations
    counts: ModelCounts
    descriptions: ModelDescriptions
    dates: ModelDates
    financials: ModelFinancials

    urls: ModelURLs

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
                'identifiers': data,
                'names': data,
                'location': data,
                'counts': data,
                'descriptions': data,
                'dates': data,
                'financials': data,
                'legal_information': data,
                'contacts': data,
                'urls': data,
            }
        return data


if __name__ == '__main__':
    doc = {
        'company_id': 'cmp_000001',
        'country': 'Finland',
        'website_url': 'www.nordic-fintech-solutions.ai',
        'employee_count': 240,
        'industry': 'Fintech',
        'short_description': None,
        'revenue_range': '100M-500M',
        'founded_year': 2018,
    }
    record = BronzeSchemaRecord(**doc)
    print(record.model_dump())
