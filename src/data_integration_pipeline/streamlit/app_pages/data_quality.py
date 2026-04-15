"""Data Quality — integration report, audit rules, schema info, and field completeness."""

import json
import streamlit as st

from clients import get_delta_client, get_query_client
import polars as pl

from data_integration_pipeline.settings import STREAMLIT_CACHE_TTL

st.header('Data Quality')


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _pg_relation_exists(name: str) -> bool:
    """Return True if a table, view, or materialized view exists."""
    rows = get_query_client().pg_client.execute(
        'SELECT 1 FROM information_schema.tables WHERE table_name = %s UNION ALL SELECT 1 FROM pg_matviews WHERE matviewname = %s LIMIT 1',
        (name, name),
    )
    return len(rows) > 0


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _fetch_report(view_name: str) -> dict:
    df = get_query_client().pg_client.read_table(view_name)
    return df.row(0, named=True)


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _fetch_field_completeness(table: str, columns: list[str]) -> tuple[int, list[tuple]]:
    pg = get_query_client().pg_client
    total = pg.get_count(table)
    if total == 0:
        return 0, []
    count_exprs = ', '.join(f'COUNT("{col}") AS "{col}"' for col in columns)
    sql = f'SELECT {count_exprs} FROM "{table}"'
    row = pg.execute(sql)[0]
    return total, list(zip(columns, row, strict=True))


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _fetch_delta_schema(table_path: str) -> list[dict]:
    schema = get_delta_client().get_schema(table_path)
    return [{'Column': field.name, 'Type': str(field.type), 'Nullable': field.nullable} for field in schema]


def _render_coverage_tab(metrics: dict[str, int], total: int, caption: str) -> None:
    """Render a coverage bar chart + table for a set of non-null count metrics."""
    st.caption(caption)
    if not metrics:
        st.info('No metrics available.')
        return
    df = pl.DataFrame(
        {
            'Field': list(metrics.keys()),
            'Non-null': list(metrics.values()),
            'Coverage %': [round(100 * v / total, 1) for v in metrics.values()],
        }
    ).with_columns(pl.col('Coverage %').cast(pl.Float64, strict=False))
    st.bar_chart(df, x='Field', y='Coverage %', horizontal=True)
    st.dataframe(df.to_dicts(), width='stretch', hide_index=True)


def _normalize_partition_distribution(value: object) -> dict[str, int]:
    """Convert report JSON output into a sorted dictionary for rendering."""
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}
    if not isinstance(value, dict):
        return {}
    parsed: dict[str, int] = {}
    for key, count in value.items():
        try:
            parsed[str(key)] = int(count)
        except (TypeError, ValueError):
            continue
    return dict(sorted(parsed.items(), key=lambda item: item[0]))


# ---------------------------------------------------------------------------
# 1. Integration report (from integrated_records_report view)
# ---------------------------------------------------------------------------

_REPORT_VIEW = 'integrated_records_report'

try:
    _show_report = _pg_relation_exists(_REPORT_VIEW)
except Exception as e:
    _show_report = False
    st.error(f'Could not check for report view `{_REPORT_VIEW}`: {e}')

if _show_report:
    st.subheader('Integration Report')
    st.caption('Summary metrics from the `integrated_records_report` Postgres view. Run `dip pg-report` to refresh.')

    try:
        row = _fetch_report(_REPORT_VIEW)
        total = row.get('total_records', 0) or 0

        if total == 0:
            st.info('The report view exists but the underlying table has no records yet.')
        else:
            col_counts, col_uniqueness, col_consistency = st.columns(3)

            with col_counts:
                st.metric('Total Records', f'{total:,}')
                st.metric('Distinct Company IDs', f'{row.get("distinct_company_ids", 0):,}')
                st.metric('Distinct URLs', f'{row.get("distinct_urls", 0):,}')

            with col_uniqueness:
                dupes_cid = row.get('duplicate_company_ids', 0) or 0
                dupes_url = row.get('duplicate_urls', 0) or 0
                st.metric(
                    'Duplicate Company IDs',
                    f'{dupes_cid:,}',
                    delta=None if dupes_cid == 0 else f'{dupes_cid} duplicates',
                    delta_color='off' if dupes_cid == 0 else 'inverse',
                )
                st.metric(
                    'Duplicate URLs',
                    f'{dupes_url:,}',
                    delta=None if dupes_url == 0 else f'{dupes_url} duplicates',
                    delta_color='off' if dupes_url == 0 else 'inverse',
                )

            with col_consistency:
                rev_bad = row.get('revenue_bounds_inconsistent', 0) or 0
                st.metric(
                    'Revenue Bounds Inconsistent',
                    f'{rev_bad:,}',
                    delta=None if rev_bad == 0 else f'{rev_bad} inconsistent',
                    delta_color='off' if rev_bad == 0 else 'inverse',
                )

            partition_distribution = _normalize_partition_distribution(row.get('iso2_country_code_distribution'))
            if partition_distribution:
                st.markdown('---')
                st.subheader('Partition Key Distribution')
                st.caption('Row distribution by partition key (`iso2_country_code`) from the report view.')
                distribution_df = pl.DataFrame(
                    {
                        'Partition Key': list(partition_distribution.keys()),
                        'Records': list(partition_distribution.values()),
                        'Share %': [round(100 * count / total, 2) for count in partition_distribution.values()],
                    }
                ).with_columns(pl.col('Share %').cast(pl.Float64, strict=False))
                st.dataframe(distribution_df.to_dicts(), width='stretch', hide_index=True)
                st.bar_chart(distribution_df, x='Partition Key', y='Share %')

            st.markdown('---')

            d1_tab, d2_tab, d3_tab = st.tabs(['Company information', 'Text enrichment', 'Embedding enrichment'])

            with d1_tab:
                d1_metrics = {k.replace('d1_has_', ''): v for k, v in row.items() if k.startswith('d1_has_')}
                _render_coverage_tab(d1_metrics, total, 'Non-null counts for key seed (Dataset 1) fields. Higher means better completeness.')

            with d2_tab:
                d2_metrics = {k.replace('d2_has_', ''): v for k, v in row.items() if k.startswith('d2_has_')}
                _render_coverage_tab(d2_metrics, total, 'Non-null counts for Dataset 2 (text enrichment) fields. Higher means more successful joins.')

            with d3_tab:
                d3_metrics = {k.replace('d3_has_', ''): v for k, v in row.items() if k.startswith('d3_has_')}
                _render_coverage_tab(d3_metrics, total, 'Non-null counts for Dataset 3 (embedding) fields. Higher means more successful joins.')

    except Exception as e:
        st.error(f'Failed to load integration report: {e}')


# ---------------------------------------------------------------------------
# 2. Audit expectation rules (static mirror of DataAuditor definitions)
# ---------------------------------------------------------------------------

st.subheader('Audit Expectation Rules')

st.caption(
    'These rules are enforced by the Great Expectations auditor on silver Delta tables. '
    'The table below is a static summary derived from the codebase.'
)

_EXPECTATION_RULES = [
    {'Column Pattern': 'Primary key (per-table)', 'Expectation': 'Values not null', 'Threshold': 'mostly 1.0', 'Severity': 'critical'},
    {'Column Pattern': 'Primary key (per-table)', 'Expectation': 'Values are unique', 'Threshold': '—', 'Severity': 'critical'},
    {'Column Pattern': 'loc__iso2_country_code', 'Expectation': 'Value lengths equal 2', 'Threshold': 'mostly 0.98', 'Severity': 'critical'},
    {'Column Pattern': 'loc__iso2_country_code', 'Expectation': 'Values not null', 'Threshold': 'mostly 0.98', 'Severity': 'critical'},
    {'Column Pattern': 'loc__country', 'Expectation': 'Values not null', 'Threshold': 'mostly 0.95', 'Severity': 'warning'},
    {'Column Pattern': 'url__request_url', 'Expectation': 'Values not null', 'Threshold': 'mostly 0.55', 'Severity': 'warning'},
    {'Column Pattern': 'url__request_url', 'Expectation': 'Match regex (http URL)', 'Threshold': 'mostly 0.85', 'Severity': 'warning'},
    {'Column Pattern': 'url__normalized_request_url', 'Expectation': 'Values not null', 'Threshold': 'mostly 0.50', 'Severity': 'warning'},
    {'Column Pattern': 'url__normalized_request_url', 'Expectation': 'Length between 1–2048', 'Threshold': 'mostly 0.90', 'Severity': 'warning'},
    {'Column Pattern': 'url__partition_key_url', 'Expectation': 'Values not null', 'Threshold': 'mostly 0.50', 'Severity': 'warning'},
    {'Column Pattern': 'url__partition_key_url', 'Expectation': 'Length between 1–2048', 'Threshold': 'mostly 0.90', 'Severity': 'warning'},
    {'Column Pattern': 'cnt__employees', 'Expectation': 'Between 0–10,000,000', 'Threshold': 'mostly 0.97', 'Severity': 'warning'},
    {'Column Pattern': 'dt__founded_year', 'Expectation': 'Between 1000–current year', 'Threshold': 'mostly 0.98', 'Severity': 'warning'},
    {'Column Pattern': 'fin__revenue_quantity_*', 'Expectation': 'Between 0–10^15', 'Threshold': 'mostly 0.96', 'Severity': 'warning'},
    {'Column Pattern': 'desc__description_*', 'Expectation': 'Values not null', 'Threshold': 'mostly 0.35', 'Severity': 'info'},
    {
        'Column Pattern': 'desc__product_services / market_niches / business_model / industry',
        'Expectation': 'Values not null',
        'Threshold': 'mostly 0.35',
        'Severity': 'info',
    },
]

st.dataframe(_EXPECTATION_RULES, width='content', hide_index=True)


# ---------------------------------------------------------------------------
# 3. Delta table schemas
# ---------------------------------------------------------------------------

st.subheader('Silver Delta Table Schemas')

_SILVER_TABLES = [
    ('Dataset 1', 'silver/dataset_1/records.delta'),
    ('Dataset 2', 'silver/dataset_2/records.delta'),
    ('Dataset 3', 'silver/dataset_3/records.delta'),
    ('Integrated', 'silver/integrated/records.delta'),
]

schema_tabs = st.tabs([name for name, _ in _SILVER_TABLES])

for tab, (_, table_path) in zip(schema_tabs, _SILVER_TABLES, strict=True):
    with tab:
        try:
            rows = _fetch_delta_schema(table_path)
            st.dataframe(rows, width='content', hide_index=True)
        except Exception as e:
            st.warning(f'Could not read schema for {table_path}: {e}')


# ---------------------------------------------------------------------------
# 4. Field completeness (gold — Postgres integrated_records)
# ---------------------------------------------------------------------------

_GOLD_TABLE = 'integrated_records'

_GOLD_DISPLAY_COLUMNS = [
    'company_id',
    'country',
    'iso2_country_code',
    'request_url',
    'normalized_request_url',
    'employees_count',
    'industry',
    'description_short',
    'founded_year',
    'revenue_quantity_lower_bound',
    'revenue_quantity_upper_bound',
    'description_long',
    'product_services',
    'market_niches',
    'business_model',
]

try:
    _show_completeness = _pg_relation_exists(_GOLD_TABLE)
except Exception as e:
    _show_completeness = False
    st.error(f'Could not check for table `{_GOLD_TABLE}`: {e}')

if _show_completeness:
    st.subheader('Gold Field Completeness')
    st.caption('Percentage of non-null values per column in the Postgres `integrated_records` table.')

    try:
        total, col_counts = _fetch_field_completeness(_GOLD_TABLE, _GOLD_DISPLAY_COLUMNS)
        if total == 0:
            st.info('No records in Postgres yet.')
        else:
            completeness = [
                {'Column': col, 'Non-null': cnt, 'Total': total, 'Completeness %': round(100 * cnt / total, 1)} for col, cnt in col_counts
            ]
            st.dataframe(completeness, width='content', hide_index=True)

            chart_df = pl.DataFrame(completeness).select('Column', 'Completeness %')
            chart_df = chart_df.with_columns(pl.col('Completeness %').cast(pl.Float64, strict=False))
            st.bar_chart(chart_df, x='Column', y='Completeness %', horizontal=True)
    except Exception as e:
        st.error(f'Failed to load field completeness: {e}')
