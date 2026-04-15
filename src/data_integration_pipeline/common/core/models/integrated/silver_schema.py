from __future__ import annotations

from datetime import datetime
from typing import ClassVar, Optional

from pydantic import AliasPath, Field

from data_integration_pipeline.common.core.models.templates.base_model_embedding import BaseModelEmbedding
from data_integration_pipeline.common.core.models.templates.base_models import BaseSilverSchemaRecord
from data_integration_pipeline.common.core.models.utils.pydantic_utils import SoftNonNegativeInt
from data_integration_pipeline.settings import LOAD_LDTS_COLUMN, EMBEDDING_LDTS_COLUMN


class SilverSchemaRecord(BaseSilverSchemaRecord):
    """
    Flat silver row built from integrated bronze ``model_dump()``:
    ``dataset_1``, ``dataset_2``, ``dataset_3`` each hold that source's silver dict.
    Dataset 1 fields keep their original names; dataset 2 and 3 use ``d2__`` / ``d3__`` prefixes
    to avoid collisions (e.g. multiple ``url__*`` columns).
    """

    # these are all class that are used for the integration processor
    _join_key: ClassVar[str] = 'url__normalized_request_url'
    _seed_data_source: ClassVar[str] = 'dataset_1'
    # these are the primary key and partition key for the integrated silver table
    _primary_key: ClassVar[str] = 'id__company'
    _partition_key: ClassVar[str] = 'loc__iso2_country_code'

    load_ldts: Optional[datetime] = Field(default=None, validation_alias=AliasPath('dataset_1', LOAD_LDTS_COLUMN), repr=False)

    # --- DATASET 1 (canonical entity / registry fields) ---
    id__company: str = Field(validation_alias=AliasPath('dataset_1', 'id__company'))

    loc__country: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'loc__country'))
    loc__iso2_country_code: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'loc__iso2_country_code'))

    url__request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'url__request_url'))
    url__normalized_request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'url__normalized_request_url'))

    cnt__employees: Optional[int] = Field(default=None, validation_alias=AliasPath('dataset_1', 'cnt__employees'))

    desc__industry: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'desc__industry'))
    desc__description_short: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'desc__description_short'))

    dt__founded_year: Optional[SoftNonNegativeInt] = Field(default=None, validation_alias=AliasPath('dataset_1', 'dt__founded_year'))

    fin__revenue_quantity_lower_bound: Optional[SoftNonNegativeInt] = Field(
        default=None, validation_alias=AliasPath('dataset_1', 'fin__revenue_quantity_lower_bound')
    )
    fin__revenue_quantity_upper_bound: Optional[SoftNonNegativeInt] = Field(
        default=None, validation_alias=AliasPath('dataset_1', 'fin__revenue_quantity_upper_bound')
    )
    fin__revenue_unit_lower_bound: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'fin__revenue_unit_lower_bound'))
    fin__revenue_unit_upper_bound: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_1', 'fin__revenue_unit_upper_bound'))

    desc__description_long: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_2', 'desc__description_long'))
    desc__product_services: Optional[list[str]] = Field(default=None, validation_alias=AliasPath('dataset_2', 'desc__product_services'))
    desc__market_niches: Optional[list[str]] = Field(default=None, validation_alias=AliasPath('dataset_2', 'desc__market_niches'))
    desc__business_model: Optional[list[str]] = Field(default=None, validation_alias=AliasPath('dataset_2', 'desc__business_model'))

    emb__global_vector: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('dataset_3', 'emb__global_vector'), repr=False)
    emb__description_long: Optional[BaseModelEmbedding] = Field(
        default=None, validation_alias=AliasPath('dataset_3', 'emb__description_long'), repr=False
    )
    emb__description_short: Optional[BaseModelEmbedding] = Field(
        default=None, validation_alias=AliasPath('dataset_3', 'emb__description_short'), repr=False
    )
    emb__product_services: Optional[BaseModelEmbedding] = Field(
        default=None, validation_alias=AliasPath('dataset_3', 'emb__product_services'), repr=False
    )
    emb__market_niches: Optional[BaseModelEmbedding] = Field(default=None, validation_alias=AliasPath('dataset_3', 'emb__market_niches'), repr=False)
    emb__business_model: Optional[BaseModelEmbedding] = Field(
        default=None, validation_alias=AliasPath('dataset_3', 'emb__business_model'), repr=False
    )
    embedding_ldts: Optional[datetime] = Field(default=None, validation_alias=AliasPath('dataset_3', f'dt__{EMBEDDING_LDTS_COLUMN}'), repr=False)

    # TODO in a real world scenario, where we actually do ER, these could be a good source of enrichment (alternative entity urls), but since our ER is only based on URL, it doesn't really make sense to include these
    # --- DATASET 2 (enrichment; prefixed) ---
    d2__url__request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_2', 'url__request_url'), exclude=True, repr=False)
    d2__url__normalized_request_url: Optional[str] = Field(
        default=None, validation_alias=AliasPath('dataset_2', 'url__normalized_request_url'), exclude=True, repr=False
    )
    d2__url__partition_key_url: Optional[str] = Field(
        default=None, validation_alias=AliasPath('dataset_2', 'url__partition_key_url'), exclude=True, repr=False
    )
    # --- DATASET 3 (embeddings; prefixed) ---
    d3__url__request_url: Optional[str] = Field(default=None, validation_alias=AliasPath('dataset_3', 'url__request_url'), exclude=True, repr=False)
    d3__url__normalized_request_url: Optional[str] = Field(
        default=None, validation_alias=AliasPath('dataset_3', 'url__normalized_request_url'), exclude=True, repr=False
    )
    d3__url__partition_key_url: Optional[str] = Field(
        default=None, validation_alias=AliasPath('dataset_3', 'url__partition_key_url'), exclude=True, repr=False
    )


if __name__ == '__main__':
    from data_integration_pipeline.common.core.models.dataset_1.metamodel import Dataset1MetaModel
    from data_integration_pipeline.common.core.models.dataset_2.metamodel import Dataset2MetaModel
    from data_integration_pipeline.common.core.models.dataset_3.metamodel import Dataset3MetaModel
    from data_integration_pipeline.common.core.models.integrated.bronze_schema import BronzeSchemaRecord as IntegratedBronzeSchemaRecord

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
    r1 = Dataset1MetaModel.from_raw_record(raw_record=dataset_1_doc)
    r2 = Dataset2MetaModel.from_raw_record(raw_record=dataset_2_doc)
    r3 = Dataset3MetaModel.from_raw_record(raw_record=dataset_3_doc)
    integrated_bronze = IntegratedBronzeSchemaRecord(
        dataset_1=r1.silver_record,
        dataset_2=r2.silver_record,
        dataset_3=r3.silver_record,
    )
    record = SilverSchemaRecord(**integrated_bronze.model_dump())
    print(record)
    print(record.model_dump())
