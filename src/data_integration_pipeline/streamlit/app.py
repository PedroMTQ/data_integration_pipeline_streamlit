"""Streamlit POC — Data Integration Pipeline explorer and search UI."""

import streamlit as st

st.set_page_config(
    page_title='Data Integration Pipeline',
    page_icon=':mag:',
    layout='wide',
)


overview_page = st.Page('app_pages/overview.py', title='Pipeline Overview', icon='📊', default=True)
quality_page = st.Page('app_pages/data_quality.py', title='Data Quality', icon='🛡')
search_page = st.Page('app_pages/search.py', title='Search & Retrieval', icon='🔍')


nav = st.navigation([overview_page, quality_page, search_page])
nav.run()
