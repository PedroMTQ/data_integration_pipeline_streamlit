"""Pipeline Overview — architecture diagram and summary statistics."""

import streamlit as st
from clients import get_delta_client, get_query_client
from data_integration_pipeline.settings import STREAMLIT_CACHE_TTL


st.header('Pipeline Overview')

# ---------------------------------------------------------------------------
# Architecture diagram
# ---------------------------------------------------------------------------

st.subheader('Architecture')

_ARCH_TEXT = """
| Stage | Component | Description |
|-------|-----------|-------------|
| **Raw Sources** | Dataset 1 | Company registry (identifiers, location, financials) |
| | Dataset 2 | Enrichment text (descriptions, products, niches) |
| | Dataset 3 | Pre-computed embeddings |
| **Bronze** | Upload & Chunk | Ingest raw JSON → chunked Parquet on S3 |
| | Validate & Process | Schema coercion, basic cleaning → Delta tables |
| **Silver** | dataset_1 / dataset_2 / dataset_3 | Individual silver Delta tables per source |
| | integrated records | Left-join of D1 + D2 + D3 on `normalized_request_url` → unified Delta |
| **Gold** | PostgreSQL | Serving layer (`integrated_records` table) with upserts |
| | Elasticsearch | Search index (text + embeddings) for two-hop retrieval |
"""

st.markdown(_ARCH_TEXT)

st.caption('Data flows top-to-bottom: Raw → Bronze → Silver → Gold (PG → ES).')

# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

st.subheader('Record Counts')

_SILVER_TABLES = [
    ('Dataset 1', 'silver/dataset_1/records.delta'),
    ('Dataset 2', 'silver/dataset_2/records.delta'),
    ('Dataset 3', 'silver/dataset_3/records.delta'),
    ('Integrated Silver', 'silver/integrated/records.delta'),
]


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _get_delta_count(table_path: str):
    return get_delta_client().get_count(table_path)


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _get_pg_count():
    return get_query_client().pg_client.get_count('integrated_records')


@st.cache_data(ttl=STREAMLIT_CACHE_TTL, show_spinner=False)
def _get_es_count():
    return get_query_client().es_client.count_all()


_GOLD_SOURCES = [
    ('Postgres (Gold)', _get_pg_count),
    ('Elasticsearch', _get_es_count),
]

cols = st.columns(len(_SILVER_TABLES) + len(_GOLD_SOURCES))

for idx, (name, table_path) in enumerate(_SILVER_TABLES):
    try:
        count = _get_delta_count(table_path)
    except Exception as e:
        st.warning(f'Could not fetch count for {name}: {e}')
        count = '—'
    cols[idx].metric(label=name, value=count)

for idx, (name, fn) in enumerate(_GOLD_SOURCES):
    try:
        count = fn()
    except Exception as e:
        st.warning(f'Could not fetch count for {name}: {e}')
        count = '—'
    cols[len(_SILVER_TABLES) + idx].metric(label=name, value=count)
