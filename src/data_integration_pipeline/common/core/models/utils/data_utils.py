from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Optional

import country_converter


_NOT_FOUND = 'not found'
COUNTRY_CONVERTER = country_converter.CountryConverter()


def get_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_country_converter_miss(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().casefold() == _NOT_FOUND:
        return True
    return isinstance(value, str) and not value.strip()


@lru_cache(maxsize=4096)
def _standardize_country_information_cached(normalized_key: str) -> tuple[Optional[str], Optional[str]]:
    """Resolve country once per normalized input; LRU-capped to avoid unbounded memory."""
    standardized_country_name = COUNTRY_CONVERTER.convert(normalized_key, to='name_short')
    if _is_country_converter_miss(standardized_country_name):
        return (None, None)
    standardized_country_code = COUNTRY_CONVERTER.convert(standardized_country_name, to='ISO2')
    if _is_country_converter_miss(standardized_country_code):
        return (None, str(standardized_country_name).strip() or None)
    code_str = str(standardized_country_code).strip()
    if len(code_str) == 2 and code_str.isalpha():
        return (code_str.upper(), str(standardized_country_name).strip())
    return (None, str(standardized_country_name).strip() or None)


def standardize_country_information(country_name: Optional[str]) -> dict:
    if country_name is None or not str(country_name).strip():
        return {'iso2_country_code': None, 'country_name': None}
    normalized = str(country_name).casefold().strip()
    if not normalized:
        return {'iso2_country_code': None, 'country_name': None}
    iso2, name = _standardize_country_information_cached(normalized)
    return {'iso2_country_code': iso2, 'country_name': name}


if __name__ == '__main__':
    country_info = standardize_country_information(country_name='Finland')
    print(country_info)
