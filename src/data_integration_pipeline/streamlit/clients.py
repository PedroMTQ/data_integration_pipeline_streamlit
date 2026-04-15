"""Cached client singletons shared across all Streamlit pages."""

import streamlit as st


@st.cache_resource
def get_query_client():
    from data_integration_pipeline.gold.io.query_client import QueryClient

    return QueryClient()


@st.cache_resource
def get_delta_client():
    from data_integration_pipeline.common.io.delta_client import DeltaClient

    return DeltaClient()
