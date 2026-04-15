from typing import ClassVar

import pyarrow as pa
from data_integration_pipeline.common.core.models.dataset_2.bronze_schema import BronzeSchemaRecord
from data_integration_pipeline.common.core.models.dataset_2.silver_schema import SilverSchemaRecord
from data_integration_pipeline.common.core.models.templates.base_models import BaseMetaModel, BaseBronzeSchemaRecord, BaseSilverSchemaRecord


class Dataset2MetaModel(BaseMetaModel):
    data_source: ClassVar[str] = 'dataset_2'
    _bronze_schema: ClassVar[BaseBronzeSchemaRecord] = BronzeSchemaRecord
    _silver_schema: ClassVar[BaseSilverSchemaRecord] = SilverSchemaRecord
    _raw_schema: ClassVar[pa.Schema] = pa.schema(
        [
            ('url', pa.string()),
            ('business_description', pa.string()),
            ('product_services', pa.string()),
            ('market_niches', pa.string()),
            ('business_model', pa.string()),
        ]
    )


if __name__ == '__main__':
    doc = {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection, risk analytics, and digital banking workflows for financial institutions.',
        'product_services': 'AI underwriting\nFraud detection platform\nRisk monitoring\nBanking analytics dashboards',
        'market_niches': 'Digital Banking\nPayments\nFinancial Infrastructure\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }
    record = Dataset2MetaModel.from_raw_record(raw_record=doc)
    print('1', record.bronze_record)
    print('2', record.silver_record)
    record = Dataset2MetaModel.from_bronze_record(bronze_record=record.bronze_record)
    print('3', record)
    record = Dataset2MetaModel.from_silver_record(silver_record=record.silver_record)
    print('4', record)
