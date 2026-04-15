from typing import Annotated, Any, Optional
from pydantic import (
    AfterValidator,
    HttpUrl,
    NonNegativeInt,
    PlainValidator,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WrapValidator,
)

import re
from string import punctuation

punctuation_set = set(punctuation)
for char_str in ['(', ')', '[', ']']:
    punctuation_set.remove(char_str)
punctuation_set.add(' ')
ONLY_DIGIT_AND_PUNCTUATION_PATTERN = re.compile(r'[\d{}\s]+$'.format(re.escape(punctuation)))

_HTTP_URL_ADAPTER = TypeAdapter(HttpUrl)


def empty_non_valid(value: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> Any:
    try:
        return handler(value)
    except (ValidationError, ValueError, TypeError) as e:
        return handler(None)


def is_valid_string(value: Any, info: ValidationInfo):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        # Postal codes are often numeric-only, so they must not be nulled.
        if ONLY_DIGIT_AND_PUNCTUATION_PATTERN.fullmatch(stripped) and info.field_name not in {'postal_code', 'zip_code'}:
            raise ValueError(f"The field '{info.field_name}' cannot contain only digits or punctuation")
        return stripped
    return value


# WrapValidator runs around inner validation; on ValidationError/ValueError/TypeError, re-validates as None.
SoftNonNegativeInt = Annotated[Optional[NonNegativeInt], WrapValidator(empty_non_valid)]
SoftStr = Annotated[Optional[str], AfterValidator(is_valid_string), WrapValidator(empty_non_valid)]
