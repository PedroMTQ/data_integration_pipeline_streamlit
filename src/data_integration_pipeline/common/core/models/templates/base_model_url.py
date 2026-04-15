from __future__ import annotations

from typing import Optional

from pydantic import (
    computed_field,
    field_validator,
)
from urllib.parse import urlparse
from w3lib.url import canonicalize_url

from data_integration_pipeline.common.core.models.templates.base_models import BaseInnerModel




class BaseModelURL(BaseInnerModel):
    # TODO I'd add url validation, but it's not needed for this use case
    @staticmethod
    def normalize_url(url: str) -> str:
        res = canonicalize_url(url)
        parsed_url = urlparse(res)
        # removes http:// and https://
        scheme = f'{parsed_url.scheme}://'
        res = parsed_url._replace(fragment='').geturl().replace(scheme, '', 1).rstrip('/')
        res = res.removeprefix('www.')
        return res.lower()

    @computed_field(description='Normalized website address')
    def normalized_request_url(self) -> Optional[str]:
        if self.request_url:
            return BaseModelURL.normalize_url(str(self.request_url))
        return None

    @staticmethod
    def get_partition_key(normalized_request_url: Optional[str]) -> str | None:
        # this is an ok interim solution, but you would want to have a better partition key that adds better balance between partitions
        if not normalized_request_url:
            return None
        if len(normalized_request_url) < 5:
            return normalized_request_url
        return normalized_request_url[0:5]



if __name__ == '__main__':
    doc = {
        'website_url': 'www.nordic-fintech-solutions.ai',
    }
    record = BaseModelURL(**doc)
    print(record)
