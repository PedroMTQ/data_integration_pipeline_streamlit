from typing import ClassVar

from pydantic import BaseModel

from data_integration_pipeline.common.core.models.integrated.bronze_schema import BronzeSchemaRecord
from data_integration_pipeline.common.core.models.integrated.gold_schema import GoldSchemaRecord
from data_integration_pipeline.common.core.models.integrated.es_schema import ElasticsearchSchemaRecord
from data_integration_pipeline.common.core.models.integrated.silver_schema import SilverSchemaRecord
from data_integration_pipeline.common.core.models.templates.base_models import BaseMetaModel, BaseBronzeSchemaRecord


class IntegratedMetaModel(BaseMetaModel):
    data_source: ClassVar[str] = 'integrated'
    _bronze_schema: ClassVar[BaseBronzeSchemaRecord] = BronzeSchemaRecord
    _gold_schema: ClassVar[BaseModel] = GoldSchemaRecord
    _silver_schema: ClassVar[BaseModel] = SilverSchemaRecord
    _es_schema: ClassVar[BaseModel] = ElasticsearchSchemaRecord


if __name__ == '__main__':
    from data_integration_pipeline.common.core.models.dataset_1.metamodel import Dataset1MetaModel
    from data_integration_pipeline.common.core.models.dataset_2.metamodel import Dataset2MetaModel
    from data_integration_pipeline.common.core.models.dataset_3.metamodel import Dataset3MetaModel

    dataset_1_doc = {
        'company_id': 'cmp_000001',
        'country': 'Finland',
        'website_url': 'www.nordic-fintech-solutions.ai',
        'employee_count': 240,
        'industry': 'Fintech',
        'short_description': None,
        'revenue_range': '100M-500M',
        'founded_year': 2018,
    }
    dataset_2_doc = {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection.',
        'product_services': 'AI underwriting\nFraud detection platform',
        'market_niches': 'Digital Banking\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }
    dataset_3_doc = {
        'url': 'https://www.nordic-fintech-solutions.ai',
        'business_description_emb': [-0.3907] * 128,
        'product_services_emb': [-0.3907] * 128,
        'market_niches_emb': [-0.3907] * 128,
        'business_model_emb': [-0.3907] * 128,
        'short_description_emb': [-0.3907] * 128,
        'embedding_updated_at': '2024-10-21T06:52:31Z',
    }
    r1 = Dataset1MetaModel.from_raw_record(raw_record=dataset_1_doc)
    r2 = Dataset2MetaModel.from_raw_record(raw_record=dataset_2_doc)
    r3 = Dataset3MetaModel.from_raw_record(raw_record=dataset_3_doc)
    raw_integrated_record = {
        'dataset_1': r1.silver_record.model_dump(),
        'dataset_2': r2.silver_record.model_dump(),
        'dataset_3': r3.silver_record.model_dump(),
    }
    integrated = IntegratedMetaModel.from_raw_record(raw_record=raw_integrated_record)
    print(integrated.gold_record.model_dump())
