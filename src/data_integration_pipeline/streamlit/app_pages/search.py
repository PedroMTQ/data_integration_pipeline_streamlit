"""Search & Retrieval — natural language search with structured filters and semantic search."""

import streamlit as st
from datetime import datetime

from data_integration_pipeline.gold.io.query_client import SearchFilters, EMBEDDING_FIELDS
from data_integration_pipeline.common.io.logger import logger
from data_integration_pipeline.settings import STREAMLIT_CACHE_TTL, ELASTICSEARCH_INDEX_ALIAS
from clients import get_query_client

current_year = datetime.now().year

_GLOBAL_EMBEDDING_FIELD = 'embedding_global_vector'
_PER_FIELD_EMBEDDING_FIELDS = [f for f in EMBEDDING_FIELDS if f != _GLOBAL_EMBEDDING_FIELD]

_EMBEDDING_FIELD_LABELS = {
    'embedding_global_vector': 'Global Vector',
    'embedding_description_long': 'Business Description',
    'embedding_description_short': 'Short Description',
    'embedding_product_services': 'Products & Services',
    'embedding_market_niches': 'Market Niches',
    'embedding_business_model': 'Business Model',
}

st.header('Search & Retrieval')

QUERY_CLIENT = get_query_client()


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _es_collection_exists() -> bool:
    """Return True when the configured ES alias/index is available."""
    es = get_query_client().es_client.es_client
    # sync-es creates a concrete index and a read alias; support either check.
    return bool(es.indices.exists_alias(name=ELASTICSEARCH_INDEX_ALIAS) or es.indices.exists(index=ELASTICSEARCH_INDEX_ALIAS))


if not _es_collection_exists():
    st.error(f'Elasticsearch collection `{ELASTICSEARCH_INDEX_ALIAS}` not found. Run `dip sync-es` first.')
    st.stop()

# ---------------------------------------------------------------------------
# Search inputs
# ---------------------------------------------------------------------------

query_text = st.text_input(
    'Keywords search (BM25)',
    placeholder='e.g. fraud detection banking analytics',
    key='search_query_text',
)
semantic_query = st.text_input(
    'Semantic search',
    placeholder='e.g. AI-powered fraud detection and risk analytics for banking',
    key='search_semantic_query',
)
st.caption('Fill text only for BM25, semantic only for vector search, or both for hybrid.')

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

with st.expander('Filters', expanded=False):
    search_country = st.text_input('Country', key='search_country', placeholder='e.g. Finland, Germany, United States')
    search_iso2_country_code = st.text_input('ISO-2 Country Code', key='search_iso2_country_code', max_chars=2, placeholder='e.g. FI, DE, US')
    search_keywords = st.text_input('Keywords (comma-separated)', key='search_keywords', placeholder='e.g. healthcare, fintech, biotech')

    col1, col2 = st.columns(2)
    search_min_employees = col1.number_input('Min employees', min_value=0, value=None, placeholder='e.g. 50', step=1, key='search_min_employees')
    search_max_employees = col2.number_input('Max employees', min_value=0, value=None, placeholder='e.g. 500', step=1, key='search_max_employees')

    col3, col4 = st.columns(2)
    search_min_founded_year = col3.number_input(
        'Min founded year', min_value=1800, max_value=current_year, value=None, placeholder='e.g. 2010', step=1, key='search_min_founded_year'
    )
    search_max_founded_year = col4.number_input(
        'Max founded year',
        min_value=1800,
        max_value=current_year,
        value=None,
        placeholder=f'e.g. {current_year}',
        step=1,
        key='search_max_founded_year',
    )

    col5, col6 = st.columns(2)
    search_min_revenue = col5.number_input(
        'Min revenue',
        min_value=0,
        value=None,
        placeholder='e.g. 1000000',
        step=1_000_000,
        key='search_min_revenue',
        help='Minimum revenue lower bound',
    )
    search_max_revenue = col6.number_input(
        'Max revenue',
        min_value=0,
        value=None,
        placeholder='e.g. 100000000',
        step=1_000_000,
        key='search_max_revenue',
        help='Maximum revenue upper bound',
    )

# ---------------------------------------------------------------------------
# Search options
# ---------------------------------------------------------------------------

search_k = st.slider(
    'Top-k results',
    min_value=1,
    max_value=100,
    value=10,
    key='search_k',
    help='Maximum number of final results returned.',
)

with st.expander('Semantic search options', expanded=False):
    search_num_candidates = st.slider(
        'num_candidates',
        min_value=10,
        max_value=500,
        value=100,
        key='search_num_candidates',
        help='HNSW candidate pool size for vector search. Higher can improve recall but increases latency.',
    )
    search_semantic_mode = st.radio(
        'Embedding mode',
        options=['Global', 'Per-field'],
        index=0,
        key='search_semantic_mode',
        help='**Per-field** searches across individual description/keyword embeddings. '
        '**Global** uses a single embedding built from all concatenated text fields.',
        horizontal=True,
    )

    if search_semantic_mode == 'Per-field':
        search_embedding_fields = st.multiselect(
            'Embedding fields to search',
            options=_PER_FIELD_EMBEDDING_FIELDS,
            default=_PER_FIELD_EMBEDDING_FIELDS,
            format_func=lambda x: _EMBEDDING_FIELD_LABELS.get(x, x),
            key='search_embedding_fields',
            help='Select which per-field embeddings to use for semantic search.',
        )
    else:
        st.caption('Using global embedding (all text fields concatenated)')
        search_embedding_fields = [_GLOBAL_EMBEDDING_FIELD]

# ---------------------------------------------------------------------------
# Example query buttons
# ---------------------------------------------------------------------------

_EXAMPLE_QUERIES: list[tuple[str, SearchFilters]] = [
    (
        'Fintech in Finland — fraud detection banking analytics',
        SearchFilters(query_text='fraud detection banking analytics', country='finland', keywords=['fintech']),
    ),
    (
        'German healthcare — >100 employees, founded after 2022, diagnostics patient monitoring',
        SearchFilters(
            query_text='diagnostics patient monitoring', country='germany', keywords=['healthcare'], min_employees=100, min_founded_year=2022
        ),
    ),
    (
        'French biotech — drug discovery, revenue >100M',
        SearchFilters(query_text='drug discovery', country='france', keywords=['biotech'], min_revenue=100_000_000),
    ),
    (
        'Swedish energy — smart grids',
        SearchFilters(query_text='smart grids renewable forecasting', country='sweden', keywords=['energy']),
    ),
    (
        'US tech — data pipelines & observability',
        SearchFilters(query_text='data pipelines observability infrastructure', iso2_country_code='us', keywords=['technology']),
    ),
    (
        'Finnish healthcare — before 2005',
        SearchFilters(query_text='diagnostics', country='finland', keywords=['healthcare'], max_founded_year=2005),
    ),
    (
        'UK telecom — 5G, <20 employees',
        SearchFilters(query_text='5G analytics', country='united kingdom', keywords=['telecom'], max_employees=20),
    ),
]

_SEMANTIC_EXAMPLE_QUERIES: list[tuple[str, SearchFilters]] = [
    (
        'Semantic: AI fraud detection for banking',
        SearchFilters(semantic_query='AI-powered fraud detection and risk analytics for banking and financial institutions'),
    ),
    (
        'Semantic: Smart grid software — Sweden',
        SearchFilters(semantic_query='software platform for smart grid management and renewable energy forecasting', country='sweden'),
    ),
    (
        'Semantic: Drug discovery biotech (product & market)',
        SearchFilters(
            semantic_query='biotechnology drug discovery and pharmaceutical research platform',
            embedding_fields=['embedding_product_services', 'embedding_market_niches'],
        ),
    ),
    (
        'Hybrid: healthcare diagnostics — Germany',
        SearchFilters(
            query_text='diagnostics patient monitoring',
            semantic_query='medical device diagnostics and remote patient monitoring technology',
            country='germany',
        ),
    ),
]


def _apply_example(filters: SearchFilters) -> None:
    """Write example filter values into widget session state keys (runs as on_click callback, before widget instantiation)."""
    f = filters.model_dump()
    st.session_state['search_query_text'] = f.get('query_text') or ''
    st.session_state['search_semantic_query'] = f.get('semantic_query') or ''
    st.session_state['search_country'] = f.get('country') or ''
    st.session_state['search_iso2_country_code'] = f.get('iso2_country_code') or ''
    st.session_state['search_keywords'] = ', '.join(f.get('keywords') or [])
    st.session_state['search_min_employees'] = f.get('min_employees') or None
    st.session_state['search_max_employees'] = f.get('max_employees') or None
    st.session_state['search_min_founded_year'] = f.get('min_founded_year') or None
    st.session_state['search_max_founded_year'] = f.get('max_founded_year') or None
    st.session_state['search_min_revenue'] = f.get('min_revenue') or None
    st.session_state['search_max_revenue'] = f.get('max_revenue') or None


with st.expander('Example queries', expanded=False):
    st.caption('**Text search**')
    example_cols = st.columns(3)
    for idx, (label, example_filters) in enumerate(_EXAMPLE_QUERIES):
        col = example_cols[idx % 3]
        col.button(label, key=f'example_{idx}', width='content', on_click=_apply_example, args=(example_filters,))

    st.caption('**Semantic / hybrid search**')
    semantic_cols = st.columns(2)
    for idx, (label, example_filters) in enumerate(_SEMANTIC_EXAMPLE_QUERIES):
        col = semantic_cols[idx % 2]
        col.button(label, key=f'semantic_example_{idx}', width='content', on_click=_apply_example, args=(example_filters,))

# ---------------------------------------------------------------------------
# Build filters from form values
# ---------------------------------------------------------------------------

_IGNORED_FILTER_FIELDS = {'k', 'num_candidates', 'embedding_fields'}

parsed_keywords = [k.strip() for k in search_keywords.split(',') if k.strip()] if search_keywords else None

active_filters = SearchFilters(
    k=search_k,
    query_text=query_text or None,
    semantic_query=semantic_query or None,
    embedding_fields=search_embedding_fields if semantic_query and search_embedding_fields else None,
    num_candidates=search_num_candidates,
    country=search_country or None,
    iso2_country_code=search_iso2_country_code.upper() if search_iso2_country_code else None,
    keywords=parsed_keywords,
    min_employees=search_min_employees,
    max_employees=search_max_employees,
    min_founded_year=search_min_founded_year,
    max_founded_year=search_max_founded_year,
    min_revenue=search_min_revenue,
    max_revenue=search_max_revenue,
)
size = search_k


if not bool(active_filters):
    st.info('Enter a search query, semantic query, or apply filters to find companies.')
    st.stop()

# ---------------------------------------------------------------------------
# Execute search
# ---------------------------------------------------------------------------

st.subheader('Results')

with st.spinner('Searching...'):
    try:
        logger.info(f'Searching with filters: {active_filters}')
        results = QUERY_CLIENT.search(active_filters, size=size)
    except Exception as e:
        st.error(f'Search failed: {e}')
        results = []

if not results:
    st.warning('No results found. Try broadening your query or relaxing filters.')
    st.stop()

st.caption(f'{len(results)} result(s)')

# ---------------------------------------------------------------------------
# Result cards
# ---------------------------------------------------------------------------

for rec in results:
    with st.container(border=True):
        hdr_cols = st.columns([2, 1, 1, 1])
        hdr_cols[0].markdown(f'**`{rec.company_id}`**')
        hdr_cols[1].markdown(f':earth_americas: {rec.country or "—"}')
        hdr_cols[2].markdown(f':factory: {rec.industry or "—"}')
        hdr_cols[3].markdown(f':calendar: {rec.founded_year or "—"}')

        if rec.description_short:
            st.markdown(rec.description_short)

        detail_cols = st.columns(3)
        detail_cols[0].markdown(f'**Employees:** {rec.employees_count or "—"}')

        rev_lo = rec.revenue_quantity_lower_bound
        rev_hi = rec.revenue_quantity_upper_bound
        rev_str = f'{rev_lo:,}–{rev_hi:,}' if rev_lo is not None and rev_hi is not None else '—'
        detail_cols[1].markdown(f'**Revenue:** {rev_str}')

        if rec.request_url:
            detail_cols[2].markdown(f'**URL:** {rec.request_url}')

        with st.expander('Full details'):
            if rec.description_long:
                st.markdown(f'**Business description:** {rec.description_long}')
            if rec.product_services:
                st.markdown('**Products & Services:** ' + ', '.join(rec.product_services))
            if rec.market_niches:
                st.markdown('**Market niches:** ' + ', '.join(rec.market_niches))
            if rec.business_model:
                st.markdown('**Business model:** ' + ', '.join(rec.business_model))
