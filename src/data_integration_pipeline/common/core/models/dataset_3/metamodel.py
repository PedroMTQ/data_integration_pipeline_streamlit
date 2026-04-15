from typing import ClassVar

import pyarrow as pa

from data_integration_pipeline.common.core.models.dataset_3.bronze_schema import BronzeSchemaRecord
from data_integration_pipeline.common.core.models.dataset_3.silver_schema import SilverSchemaRecord
from data_integration_pipeline.common.core.models.templates.base_models import BaseMetaModel, BaseBronzeSchemaRecord, BaseSilverSchemaRecord


class Dataset3MetaModel(BaseMetaModel):
    data_source: ClassVar[str] = 'dataset_3'
    _bronze_schema: ClassVar[BaseBronzeSchemaRecord] = BronzeSchemaRecord
    _silver_schema: ClassVar[BaseSilverSchemaRecord] = SilverSchemaRecord
    _raw_schema: ClassVar[pa.Schema] = pa.schema(
        [
            ('url', pa.string()),
            ('business_description_emb', pa.list_(pa.float32())),
            ('product_services_emb', pa.list_(pa.float32())),
            ('market_niches_emb', pa.list_(pa.float32())),
            ('business_model_emb', pa.list_(pa.float32())),
            ('short_description_emb', pa.list_(pa.float32())),
            ('embedding_updated_at', pa.string()),
        ]
    )


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
    record = Dataset3MetaModel.from_raw_record(raw_record=doc)
    print(record)
