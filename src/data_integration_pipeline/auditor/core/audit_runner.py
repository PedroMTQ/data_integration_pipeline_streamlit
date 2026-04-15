"""
AuditRunner: orchestrates sampling, rule assembly, and GX audit execution for a Delta table.

The job layer (audit_silver.py / audit_gold.py) handles work discovery;
this module wires together the sampler, data model rules, and DataAuditor.
"""

from typing import Any, Literal

import great_expectations as gx
import pyarrow as pa
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.auditor.core.data_auditor import DataAuditor
from data_integration_pipeline.auditor.core.expectation_data_model import ModelExpectationTemplate
from data_integration_pipeline.auditor.core.metadata.audit_metadata import AuditMetadata
from data_integration_pipeline.auditor.io.delta_weighted_data_sampler import DeltaWeightedDataSampler
from data_integration_pipeline.common.core.models.model_mapper import ModelMapper
from data_integration_pipeline.settings import AUDIT_TOTAL_ROWS


class AuditRunner:
    """Runs a GX audit on a single Delta table: samples data, builds rules, executes, and exports docs."""

    def __init__(self, s3_path: str, dataset_stage: Literal['bronze', 'silver', 'gold'], target_total_rows: int = AUDIT_TOTAL_ROWS):
        self.s3_path = s3_path
        self.dataset_stage = dataset_stage
        self.target_total_rows = target_total_rows

    @staticmethod
    def get_uniqueness_rules(primary_key: str) -> list[dict]:
        """Primary-key uniqueness rule, shared across silver and gold audits."""
        return [
            {
                'patterns': [primary_key],
                'rules': [
                    ModelExpectationTemplate(
                        expectation_class=gx.expectations.ExpectColumnValuesToBeUnique,
                        expectation_kwargs={
                            'severity': 'critical',
                            'meta': {
                                'description': 'Ensures IDs dont repeat',
                                'notes': 'If this fails, there is a data duplication issue.',
                            },
                        },
                    )
                ],
            },
        ]

    def run(self, metadata: AuditMetadata) -> dict[str, Any]:
        logger.info(f'Auditing {self.s3_path}')
        data_model = ModelMapper.get_data_model(self.s3_path)
        if not data_model:
            raise ValueError(f'No data model found for {self.s3_path}')
        primary_key = ModelMapper.get_primary_key(self.s3_path)
        sampler = DeltaWeightedDataSampler(
            s3_path=self.s3_path,
            weight_column=ModelMapper.get_partition_key(self.s3_path),
            primary_key=primary_key,
            target_total_rows=self.target_total_rows,
        )
        additional_rules = self.get_uniqueness_rules(primary_key)
        data_auditor = DataAuditor(
            data_model=data_model,
            dataset_stage=self.dataset_stage,
            additional_rules=additional_rules,
            primary_key=primary_key,
        )
        data_sample: pa.RecordBatchReader = sampler.get_filtered_data(columns_filter=data_auditor.audit_columns)
        data_auditor.run(data=data_sample)
        data_auditor.export_docs()
        metadata.total_raw_records = sampler.get_total_raw_records()
        metadata.total_sampled_records = sampler.get_total_sampled_records()
        metadata.raw_data_distribution = sampler.get_raw_data_distribution()
        metadata.sample_data_distribution = sampler.get_sample_data_distribution()
        metadata.audit_columns = list(data_auditor.audit_columns)
        return metadata
