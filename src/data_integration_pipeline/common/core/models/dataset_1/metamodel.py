from typing import ClassVar


from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord
from data_integration_pipeline.common.core.models.dataset_1.silver_schema import SilverSchemaRecord
from data_integration_pipeline.common.core.models.templates.base_models import BaseMetaModel, BaseBronzeSchemaRecord, BaseSilverSchemaRecord
import pyarrow as pa


class Dataset1MetaModel(BaseMetaModel):
    data_source: ClassVar[str] = 'dataset_1'
    _bronze_schema: ClassVar[BaseBronzeSchemaRecord] = BronzeSchemaRecord
    _silver_schema: ClassVar[BaseSilverSchemaRecord] = SilverSchemaRecord
    _raw_schema: ClassVar[pa.Schema] = pa.schema(
        [
            ('company_id', pa.string()),
            ('country', pa.string()),
            ('industry', pa.string()),
            ('revenue_range', pa.string()),
            ('employee_count', pa.int64()),
            ('founded_year', pa.int64()),
            ('short_description', pa.string()),
            ('website_url', pa.string()),
        ]
    )


if __name__ == '__main__':
    doc = {
        'company_id': 'cmp_000001',
        'country': 'Finland',
        'industry': 'Fintech',
        'revenue_range': '100M-500M',
        'employee_count': 240,
        'founded_year': 2018,
        'short_description': None,
        'website_url': 'www.nordic-fintech-solutions.ai',
    }
    record = Dataset1MetaModel.from_raw_record(raw_record=doc)
    print('1', record.bronze_record)
    print('2', record.silver_record)
    print('3', record.gold_record)
    # record = Dataset1MetaModel.from_bronze_record(bronze_record=record.bronze_record)
    # print('3',record)
    # record = Dataset1MetaModel.from_silver_record(silver_record=record.silver_record)
    # print('4',record)
