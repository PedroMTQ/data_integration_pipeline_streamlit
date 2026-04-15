import fnmatch
import os
from datetime import datetime
from typing import Any, Iterator, Literal, Type

import great_expectations as gx
import pyarrow as pa
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.auditor.core.expectation_data_model import ModelExpectation, ModelExpectationTemplate
from data_integration_pipeline.common.core.models.model_mapper import BaseRecordType
from data_integration_pipeline.settings import TEMP


class DataAuditor:
    data_source_name: str = 'pandas'
    # ! tried with duckdb but kept getting this error: "list index out of range". doesn't seem very well supported

    def __init__(
        self,
        data_model: Type[BaseRecordType],
        dataset_stage: Literal['bronze', 'silver', 'gold'],
        additional_rules: list[dict] = None,
        rebuild_suite: bool = True,
        primary_key: str = None,
    ):
        self.data_model = data_model
        self.schema_columns = data_model._pa_silver_schema.names
        self.additional_rules = additional_rules
        self._rebuild_suite = rebuild_suite
        self.dataset_stage = dataset_stage
        self.audit_columns = []
        self.primary_key = primary_key
        data_suffix = f'{data_model.data_source}_{dataset_stage}'
        self.batch_definition_name = f'sample_{data_suffix}'
        self.run_name = f'audit_{data_suffix}'
        self.data_asset_name = data_model.data_source
        self.suite_name = f'suite_{data_suffix}'
        self.validation_definition_name = f'validation_definition_{data_suffix}'

        self.run_id = gx.RunIdentifier(run_name=self.run_name)
        self.context = gx.get_context(mode='file', project_root_dir=os.path.join(TEMP, 'audits'))
        self.__setup_expectations()

    def __str__(self):
        return '\n'.join(
            [f'DataAuditor(schema_columns={self.schema_columns}', f'audit_columns={self.audit_columns}', f'expectations={self.expectations}']
        )

    def _get_expectations_definitions(self) -> list[dict[str, Any]]:
        """Build GX expectation templates keyed to flat ``_pa_silver_schema`` column names."""
        current_year = datetime.now().year

        res: list[dict[str, Any]] = []

        if self.primary_key and self.primary_key in self.schema_columns:
            res.append(
                {
                    'patterns': [self.primary_key],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={
                                'mostly': 1.0,
                                'severity': 'critical',
                                'meta': {
                                    'description': 'Silver primary key must be present',
                                    'notes': 'Uniqueness is enforced separately in AuditRunner.',
                                },
                            },
                        ),
                    ],
                }
            )

        res.extend(
            [
                {
                    'patterns': ['loc__iso2_country_code'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValueLengthsToEqual,
                            expectation_kwargs={
                                'value': 2,
                                'mostly': 0.98,
                                'severity': 'critical',
                                'meta': {
                                    'description': 'ISO2 country codes are exactly two characters',
                                    'notes': 'If this fails, downstream location mapping may break.',
                                },
                            },
                        ),
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={
                                'mostly': 0.98,
                                'severity': 'critical',
                                'meta': {
                                    'description': 'Partition country code should rarely be null when present on row',
                                    'notes': 'Matches silver location.iso2_country_code contract.',
                                },
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['loc__country'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={
                                'mostly': 0.95,
                                'severity': 'warning',
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['url__request_url'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={'mostly': 0.55, 'severity': 'warning'},
                        ),
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToMatchRegex,
                            expectation_kwargs={
                                'regex': r'^https?://[^\s/$.?#].[^\s]*$',
                                'mostly': 0.85,
                                'severity': 'warning',
                                'meta': {'description': 'Request URL should look like an absolute http(s) URL when set'},
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['url__normalized_request_url'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={'mostly': 0.5, 'severity': 'warning'},
                        ),
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValueLengthsToBeBetween,
                            expectation_kwargs={
                                'min_value': 1,
                                'max_value': 2048,
                                'mostly': 0.9,
                                'severity': 'warning',
                                'meta': {
                                    'description': 'Normalized / domain URLs are non-empty strings when present',
                                    'notes': 'Host-only values are allowed; no strict https:// prefix.',
                                },
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['cnt__employees'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToBeBetween,
                            expectation_kwargs={
                                'min_value': 0,
                                'max_value': 10_000_000,
                                'mostly': 0.97,
                                'severity': 'warning',
                                'meta': {'description': 'Employee count aligns with SoftNonNegativeInt / sensible upper bound'},
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['dt__founded_year'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToBeBetween,
                            expectation_kwargs={
                                'min_value': 1000,
                                'max_value': current_year + 1,
                                'mostly': 0.98,
                                'severity': 'warning',
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['*revenue_quantity*'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToBeBetween,
                            expectation_kwargs={
                                'min_value': 0,
                                'max_value': 10**15,
                                'mostly': 0.96,
                                'severity': 'warning',
                            },
                        ),
                    ],
                },
                {
                    'patterns': ['desc__description_*'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={'mostly': 0.35, 'severity': 'info'},
                        ),
                    ],
                },
                {
                    'patterns': ['desc__product_services', 'desc__market_niches', 'desc__business_model', 'desc__industry'],
                    'rules': [
                        ModelExpectationTemplate(
                            expectation_class=gx.expectations.ExpectColumnValuesToNotBeNull,
                            expectation_kwargs={'mostly': 0.35, 'severity': 'info'},
                        ),
                    ],
                },
            ]
        )
        if self.additional_rules:
            # We assume additional_rules follows the same {"pattern": str, "rules": [...]} format
            res.extend(self.additional_rules)
        return res

    def __setup_expectations(self):
        definitions = self._get_expectations_definitions()
        self.expectations = []
        for entry in definitions:
            # 1. Uncompress pattern to actual columns
            column_patterns = entry.get('patterns')
            rules = entry.get('rules')
            if not column_patterns:
                raise Exception(f'Invalid rule (missing pattern key): {entry}')
            if not rules:
                raise Exception(f'Invalid rule (missing rules key): {entry}')
            matched_cols = set()
            for p in column_patterns:
                matches = fnmatch.filter(self.schema_columns, p)
                matched_cols.update(matches)
            for col in matched_cols:
                expectation_template: ModelExpectationTemplate
                for expectation_template in rules:
                    if not expectation_template:
                        continue
                    # 2. Bind the template to the specific column (this triggers validation
                    try:
                        expectation_model: ModelExpectation = expectation_template.apply_to(col)
                        if col not in self.audit_columns:
                            self.audit_columns.append(col)
                        self.expectations.append(expectation_model)
                    except Exception:
                        logger.exception(
                            'Failed to add expectation for column=%s expectation_class=%s',
                            col,
                            getattr(expectation_template.expectation_class, '__name__', expectation_template.expectation_class),
                        )
                        raise

    def __setup_data_source(self):
        # Add Data Source
        try:
            self.datasource = self.context.data_sources.get(self.data_source_name)
        except KeyError:
            self.datasource = self.context.data_sources.add_pandas(name=self.data_source_name)

    def __setup_data_asset(self):

        # Add Data Asset
        try:
            self.data_asset = self.datasource.get_asset(self.data_asset_name)
        except LookupError:
            self.data_asset = self.datasource.add_dataframe_asset(name=self.data_asset_name)

    def __setup_batch_def(self):

        # Add Batch Definition (Required for GX 1.x)
        try:
            self.batch_definition = self.data_asset.get_batch_definition(self.batch_definition_name)
        except KeyError:
            self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe(name=self.batch_definition_name)

    def __setup_suite(self):
        if self._rebuild_suite:
            try:
                self.context.suites.delete(self.suite_name)
            except Exception:
                pass  # Handle if it didn't exist
            self.suite = self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))
            self.__build_suite()
        else:
            try:
                # Try to get existing suite
                self.suite = self.context.suites.get(self.suite_name)
                # Logic: If your code's definitions have changed, update the store
                self.__build_suite()
                self.context.suites.add_or_update(self.suite)
            except gx.exceptions.DataContextError:
                # Create new if it doesn't exist
                self.suite = self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))
                self.__build_suite()

    def __setup_val_definition(self):
        try:
            self.val_definition = self.context.validation_definitions.get(self.validation_definition_name)
        except gx.exceptions.DataContextError:
            self.val_definition = self.context.validation_definitions.add(
                gx.ValidationDefinition(name=self.validation_definition_name, data=self.batch_definition, suite=self.suite)
            )

    def __build_suite(self):
        for expectation_model in self.expectations:
            self.suite.add_expectation(expectation_model.expectation)

    def __process_results(self, results: list[dict]) -> bool:
        exception_failures = [r for r in results if not r['success'] and r['exception_info'].get('exception_info', {})]
        critical_failures = [r for r in results if not r['success'] and r['expectation_config']['meta'].get('severity') == 'critical']
        warning_failures = [r for r in results if not r['success'] and r['expectation_config']['meta'].get('severity') == 'warning']
        info_failures = [r for r in results if not r['success'] and r['expectation_config']['meta'].get('severity') == 'info']
        # 4. Define your "Business Logic" for success
        if exception_failures:
            logger.critical(f'❌ AUDIT FAILED: {len(exception_failures)} exceptions found.')
            # Logic to handle hard stop (e.g., return False or raise Exception)
            return False
        if critical_failures:
            logger.critical(f'❌ AUDIT FAILED: {len(critical_failures)} critical errors found.')
            # Logic to handle hard stop (e.g., return False or raise Exception)
            return False
        if warning_failures:
            logger.warning(f'⚠️ AUDIT PASSED WITH WARNINGS: {len(warning_failures)} issues detected.')
            return True
        if info_failures:
            logger.info(f'⚠️ AUDIT PASSED WITH INFO: {len(info_failures)} issues detected.')
            return True
        logger.info('✅ AUDIT PASSED: All expectations met.')
        return True

    def export_docs(self):
        self.context.build_data_docs()

    def run(self, data: Iterator[pa.RecordBatch]) -> bool:
        self.__setup_data_source()
        self.__setup_suite()
        self.__setup_data_asset()
        self.__setup_batch_def()
        self.__setup_val_definition()
        # TODO gx db engines are not great for my current setup, consider improving this. but it should be ok for the amount of data we audit
        df = pa.Table.from_batches(data).to_pandas()
        logger.info(f'Runnning audit on schema: {df.dtypes}')
        results = self.val_definition.run(batch_parameters={'dataframe': df}, run_id=self.run_id)
        return self.__process_results(results=results.get('results', {}))


if __name__ == '__main__':
    from data_integration_pipeline.auditor.io.delta_weighted_data_sampler import DeltaWeightedDataSampler
    from data_integration_pipeline.common.core.models.model_mapper import ModelMapper

    s3_path = 'silver/dataset_1/records.delta'
    data_model = ModelMapper.get_data_model(s3_path)
    s3_sampler = DeltaWeightedDataSampler(
        s3_path=s3_path,
        weight_column='loc__iso2_country_code',
        default_weight=1,
        target_total_rows=100,
        primary_key=data_model._silver_schema._primary_key,
    )
    # Consume the generator to trigger the sampling
    print('get_total_raw_records', s3_sampler.get_total_raw_records())
    print('get_raw_data_distribution', s3_sampler.get_raw_data_distribution())
    print('get_total_sampled_records', s3_sampler.get_total_sampled_records())
    print('get_sample_data_distribution', s3_sampler.get_sample_data_distribution())
    data_auditor = DataAuditor(data_model=data_model, dataset_stage='silver', primary_key=data_model._silver_schema._primary_key)
    print(data_auditor)
    # data_sample = s3_sampler.gets_data()
    # results = data_auditor.run(data=data_sample)
    # print(results)
    # data_auditor.export_docs()
