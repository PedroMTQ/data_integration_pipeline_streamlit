"""Create or replace a PostgreSQL summary view over integrated_records."""

from __future__ import annotations
import polars as pl

from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.gold.io.postgres_client import PostgresClient

_TABLE = 'integrated_records'
_VIEW = 'integrated_records_report'
_PARTITION_KEY_COL = 'iso2_country_code'
_UNKNOWN_PARTITION_KEY = '__null__'

# D1 fields — non-null coverage count shows seed data availability
_D1_FIELDS = [
    'country',
    'iso2_country_code',
    'normalized_request_url',
    'employees_count',
    'founded_year',
    'industry',
    'description_short',
]

# D2 fields — coverage count shows D1→D2 join success
_D2_FIELDS = [
    'description_long',
    'product_services',
    'market_niches',
    'business_model',
]

# D3 fields — coverage count shows D1→D3 join success
_D3_FIELDS = [
    'embedding_global_vector',
    'embedding_description_long',
    'embedding_description_short',
    'embedding_product_services',
    'embedding_market_niches',
    'embedding_business_model',
]


def _count_non_null(col: str, alias: str) -> str:
    return f'COUNT(*) FILTER (WHERE {col} IS NOT NULL)::int AS {alias}'


def _partition_distribution_json(col: str, alias: str) -> str:
    return (
        'COALESCE(('
        f'SELECT jsonb_object_agg(partition_value, record_count ORDER BY partition_value) '
        'FROM ('
        f"SELECT COALESCE({col}, '{_UNKNOWN_PARTITION_KEY}') AS partition_value, COUNT(*)::int AS record_count "
        f'FROM {_TABLE} '
        'GROUP BY 1'
        ') AS partition_counts'
        "), '{}'::jsonb) "
        f'AS {alias}'
    )


def _build_report_sql() -> str:
    cols: list[str] = []

    # --- counts ---
    cols.append('COUNT(*)::int AS total_records')
    cols.append('COUNT(DISTINCT company_id)::int AS distinct_company_ids')
    cols.append('COUNT(DISTINCT normalized_request_url)::int AS distinct_urls')
    cols.append(_partition_distribution_json(_PARTITION_KEY_COL, f'{_PARTITION_KEY_COL}_distribution'))

    # --- D1 non-null coverage (seed data availability) ---
    cols.extend(_count_non_null(c, f'd1_has_{c}') for c in _D1_FIELDS)

    # --- D1 → D2 coverage (non-null = join succeeded) ---
    cols.extend(_count_non_null(c, f'd2_has_{c}') for c in _D2_FIELDS)

    # --- D1 → D3 coverage ---
    cols.extend(_count_non_null(c, f'd3_has_{c.replace("embedding_", "emb_")}') for c in _D3_FIELDS)

    # --- uniqueness ---
    cols.append('(COUNT(*)::int - COUNT(DISTINCT company_id)::int) AS duplicate_company_ids')
    cols.append('(COUNT(*) FILTER (WHERE normalized_request_url IS NOT NULL)::int - COUNT(DISTINCT normalized_request_url)::int) AS duplicate_urls')

    # --- revenue consistency ---
    cols.append(
        'COUNT(*) FILTER (WHERE revenue_quantity_lower_bound IS NOT NULL'
        ' AND revenue_quantity_upper_bound IS NOT NULL'
        ' AND revenue_quantity_lower_bound > revenue_quantity_upper_bound)::int'
        ' AS revenue_bounds_inconsistent'
    )

    body = ',\n    '.join(cols)
    return f'CREATE OR REPLACE VIEW {_VIEW} AS\nSELECT\n    {body}\nFROM {_TABLE}'


class CreatePostgresReportJob:
    """Creates ``integrated_records_report``: a single-row summary view with
    counts, non-null coverage, uniqueness, and revenue consistency."""

    INPUT_POSTGRES_TABLE = _TABLE
    OUTPUT_POSTGRES_VIEW = _VIEW
    REPORT_SQL = _build_report_sql()

    def run(self) -> None:
        logger.info('Creating or replacing view %s from %s', self.OUTPUT_POSTGRES_VIEW, self.INPUT_POSTGRES_TABLE)
        client = PostgresClient()
        client.execute_schema(self.REPORT_SQL)
        logger.info('View %s is ready', self.OUTPUT_POSTGRES_VIEW)
        view_data = client.read_table(self.OUTPUT_POSTGRES_VIEW)
        view_data = view_data.transpose(include_header=True)
        with pl.Config(tbl_rows=50):
            logger.info('View data: %s', view_data)
        return view_data.to_dict()
