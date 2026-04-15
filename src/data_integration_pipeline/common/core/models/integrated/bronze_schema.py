from data_integration_pipeline.common.core.models.templates.base_models import BaseBronzeSchemaRecord
from data_integration_pipeline.common.core.models.dataset_1.metamodel import Dataset1MetaModel
from data_integration_pipeline.common.core.models.dataset_2.metamodel import Dataset2MetaModel
from data_integration_pipeline.common.core.models.dataset_3.metamodel import Dataset3MetaModel


class BronzeSchemaRecord(BaseBronzeSchemaRecord):
    dataset_1: Dataset1MetaModel._silver_schema
    dataset_2: Dataset2MetaModel._silver_schema
    dataset_3: Dataset3MetaModel._silver_schema


if __name__ == '__main__':
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
        'business_description': 'The company provides software for fraud detection, risk analytics, and digital banking workflows for financial institutions.',
        'product_services': 'AI underwriting\nFraud detection platform\nRisk monitoring\nBanking analytics dashboards',
        'market_niches': 'Digital Banking\nPayments\nFinancial Infrastructure\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }
    dataset_3_doc = {
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
    dataset_1_record = Dataset1MetaModel.from_raw_record(raw_record=dataset_1_doc)
    dataset_2_record = Dataset2MetaModel.from_raw_record(raw_record=dataset_2_doc)
    dataset_3_record = Dataset3MetaModel.from_raw_record(raw_record=dataset_3_doc)
    raw_integrated_record = {
        'dataset_1': dataset_1_record.silver_record,
        'dataset_2': dataset_2_record.silver_record,
        'dataset_3': dataset_3_record.silver_record,
    }
    record = BronzeSchemaRecord(**raw_integrated_record)
    print(record)
