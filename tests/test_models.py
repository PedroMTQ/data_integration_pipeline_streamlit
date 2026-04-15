"""Unit tests for Pydantic data models and transformations.

These tests run without Docker services — they verify the schema validation,
URL normalization, country standardization, revenue parsing, and model
conversion pipeline (raw → bronze → silver → gold).
"""

from __future__ import annotations

import pytest

from data_integration_pipeline.settings import EMBEDDING_DIMENSIONS


# ---------------------------------------------------------------------------
# Dataset 1 (Company Registry)
# ---------------------------------------------------------------------------


class TestDataset1Model:
    RAW = {
        'company_id': 'cmp_000001',
        'country': 'Finland',
        'industry': 'Fintech',
        'revenue_range': '100M-500M',
        'employee_count': 240,
        'founded_year': 2018,
        'short_description': None,
        'website_url': 'www.nordic-fintech-solutions.ai',
    }

    def test_bronze_validation(self):
        from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord

        record = BronzeSchemaRecord(**self.RAW)
        assert record.identifiers.company == 'cmp_000001'
        assert record.location.iso2_country_code == 'FI'
        assert record.counts.employees_count == 240

    def test_url_normalization(self):
        from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord

        record = BronzeSchemaRecord(**self.RAW)
        assert record.urls.normalized_request_url == 'nordic-fintech-solutions.ai'

    def test_revenue_parsing(self):
        from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord

        record = BronzeSchemaRecord(**self.RAW)
        assert record.financials.revenue_quantity_lower_bound == 100
        assert record.financials.revenue_quantity_upper_bound == 500
        assert record.financials.revenue_unit_lower_bound == 'M'
        assert record.financials.revenue_unit_upper_bound == 'M'

    def test_full_metamodel_conversion(self):
        from data_integration_pipeline.common.core.models.dataset_1.metamodel import Dataset1MetaModel

        meta = Dataset1MetaModel.from_raw_record(raw_record=self.RAW)
        assert meta.bronze_record is not None
        silver = meta.silver_record
        assert silver.id__company == 'cmp_000001'
        assert silver.loc__iso2_country_code == 'FI'
        assert silver.cnt__employees == 240
        assert silver.dt__founded_year == 2018

    def test_country_standardization_various(self):
        from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord

        for country_input, expected_iso2 in [
            ('Finland', 'FI'),
            ('Germany', 'DE'),
            ('United States', 'US'),
            ('France', 'FR'),
        ]:
            raw = {**self.RAW, 'country': country_input}
            record = BronzeSchemaRecord(**raw)
            assert record.location.iso2_country_code == expected_iso2, (
                f'{country_input} → expected {expected_iso2}, got {record.location.iso2_country_code}'
            )

    def test_invalid_employee_count_coerced(self):
        from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord

        raw = {**self.RAW, 'employee_count': 'not_a_number'}
        record = BronzeSchemaRecord(**raw)
        assert record.counts.employees_count is None

    def test_url_variants_normalize_to_same_key(self):
        from data_integration_pipeline.common.core.models.dataset_1.bronze_schema import BronzeSchemaRecord

        urls = [
            'www.nordic-fintech-solutions.ai',
            'https://www.nordic-fintech-solutions.ai',
            'http://nordic-fintech-solutions.ai/',
            'https://nordic-fintech-solutions.ai',
        ]
        normalized = set()
        for url in urls:
            raw = {**self.RAW, 'website_url': url}
            record = BronzeSchemaRecord(**raw)
            normalized.add(record.urls.normalized_request_url)
        assert len(normalized) == 1, f'Expected all URLs to normalize to one key, got {normalized}'


# ---------------------------------------------------------------------------
# Dataset 2 (URL Enrichment)
# ---------------------------------------------------------------------------


class TestDataset2Model:
    RAW = {
        'url': 'https://nordic-fintech-solutions.ai',
        'business_description': 'The company provides software for fraud detection.',
        'product_services': 'AI underwriting\nFraud detection platform\nRisk monitoring',
        'market_niches': 'Digital Banking\nPayments\nFinland',
        'business_model': 'B2B | subscription fees | SaaS',
    }

    def test_bronze_validation(self):
        from data_integration_pipeline.common.core.models.dataset_2.bronze_schema import BronzeSchemaRecord

        record = BronzeSchemaRecord(**self.RAW)
        assert record.descriptions.description_long == 'The company provides software for fraud detection.'
        assert len(record.descriptions.product_services) == 3
        assert len(record.descriptions.business_model) == 3

    def test_newline_splitting(self):
        from data_integration_pipeline.common.core.models.dataset_2.bronze_schema import BronzeSchemaRecord

        record = BronzeSchemaRecord(**self.RAW)
        assert 'AI underwriting' in record.descriptions.product_services
        assert 'Fraud detection platform' in record.descriptions.product_services

    def test_pipe_splitting_business_model(self):
        from data_integration_pipeline.common.core.models.dataset_2.bronze_schema import BronzeSchemaRecord

        record = BronzeSchemaRecord(**self.RAW)
        assert 'B2B' in record.descriptions.business_model
        assert 'SaaS' in record.descriptions.business_model

    def test_full_metamodel_conversion(self):
        from data_integration_pipeline.common.core.models.dataset_2.metamodel import Dataset2MetaModel

        meta = Dataset2MetaModel.from_raw_record(raw_record=self.RAW)
        silver = meta.silver_record
        assert silver.url__normalized_request_url == 'nordic-fintech-solutions.ai'
        assert silver.desc__description_long is not None
        assert isinstance(silver.desc__product_services, list)


# ---------------------------------------------------------------------------
# Dataset 3 (Embeddings)
# ---------------------------------------------------------------------------


class TestDataset3Model:
    def _make_raw(self) -> dict:
        emb = [float(i) / EMBEDDING_DIMENSIONS for i in range(EMBEDDING_DIMENSIONS)]
        return {
            'url': 'https://nordic-fintech-solutions.ai',
            'business_description_emb': emb,
            'product_services_emb': emb,
            'market_niches_emb': emb,
            'business_model_emb': emb,
            'short_description_emb': emb,
            'embedding_updated_at': '2024-10-21T06:52:31Z',
        }

    def test_bronze_validation(self):
        from data_integration_pipeline.common.core.models.dataset_3.bronze_schema import BronzeSchemaRecord

        raw = self._make_raw()
        record = BronzeSchemaRecord(**raw)
        assert record.embeddings.description_long.dimensions == EMBEDDING_DIMENSIONS
        assert len(record.embeddings.description_long.embedding) == EMBEDDING_DIMENSIONS

    def test_wrong_dimensions_raises(self):
        from data_integration_pipeline.common.core.models.dataset_3.bronze_schema import BronzeSchemaRecord

        raw = self._make_raw()
        raw['business_description_emb'] = [0.1, 0.2, 0.3]
        with pytest.raises(Exception('Invalid embedding dimensions')):
            BronzeSchemaRecord(**raw)

    def test_full_metamodel_conversion(self):
        from data_integration_pipeline.common.core.models.dataset_3.metamodel import Dataset3MetaModel

        raw = self._make_raw()
        meta = Dataset3MetaModel.from_raw_record(raw_record=raw)
        silver = meta.silver_record
        assert silver.url__normalized_request_url == 'nordic-fintech-solutions.ai'
        assert silver.emb__description_long is not None


# ---------------------------------------------------------------------------
# Integrated Model
# ---------------------------------------------------------------------------


class TestIntegratedModel:
    def test_gold_record_has_hk_and_hdiff(self):
        from data_integration_pipeline.common.core.models.dataset_1.metamodel import Dataset1MetaModel
        from data_integration_pipeline.common.core.models.dataset_2.metamodel import Dataset2MetaModel
        from data_integration_pipeline.common.core.models.dataset_3.metamodel import Dataset3MetaModel
        from data_integration_pipeline.common.core.models.integrated.bronze_schema import BronzeSchemaRecord as IntegratedBronze
        from data_integration_pipeline.common.core.models.integrated.silver_schema import SilverSchemaRecord as IntegratedSilver
        from data_integration_pipeline.common.core.models.integrated.gold_schema import GoldSchemaRecord as IntegratedGold

        emb = [float(i) / EMBEDDING_DIMENSIONS for i in range(EMBEDDING_DIMENSIONS)]
        r1 = Dataset1MetaModel.from_raw_record(
            raw_record={
                'company_id': 'cmp_001',
                'country': 'Finland',
                'industry': 'Fintech',
                'revenue_range': '100M-500M',
                'employee_count': 240,
                'founded_year': 2018,
                'short_description': None,
                'website_url': 'www.nordic-fintech-solutions.ai',
            }
        )
        r2 = Dataset2MetaModel.from_raw_record(
            raw_record={
                'url': 'https://nordic-fintech-solutions.ai',
                'business_description': 'Fraud detection software.',
                'product_services': 'AI underwriting\nFraud detection',
                'market_niches': 'Digital Banking\nFinland',
                'business_model': 'B2B | SaaS',
            }
        )
        r3 = Dataset3MetaModel.from_raw_record(
            raw_record={
                'url': 'https://nordic-fintech-solutions.ai',
                'business_description_emb': emb,
                'product_services_emb': emb,
                'market_niches_emb': emb,
                'business_model_emb': emb,
                'short_description_emb': emb,
                'embedding_updated_at': '2024-10-21T06:52:31Z',
            }
        )

        integrated_bronze = IntegratedBronze(
            dataset_1=r1.silver_record,
            dataset_2=r2.silver_record,
            dataset_3=r3.silver_record,
        )
        silver = IntegratedSilver(**integrated_bronze.model_dump())
        assert silver.id__company == 'cmp_001'
        assert silver.desc__description_long == 'Fraud detection software.'

        silver_data = silver.model_dump()
        silver_data['load_ldts'] = '2024-01-01T00:00:00Z'
        gold = IntegratedGold(**silver_data)
        assert gold.hk is not None
        assert gold.hdiff is not None
        assert gold.company_id == 'cmp_001'
        assert gold.country.lower() == 'finland'

    def test_hk_is_deterministic(self):
        from data_integration_pipeline.common.core.utils import get_hk

        hk1 = get_hk('cmp_001', 'integrated')
        hk2 = get_hk('cmp_001', 'integrated')
        assert hk1 == hk2

    def test_different_companies_get_different_hks(self):
        from data_integration_pipeline.common.core.utils import get_hk

        hk1 = get_hk('cmp_001', 'integrated')
        hk2 = get_hk('cmp_002', 'integrated')
        assert hk1 != hk2
