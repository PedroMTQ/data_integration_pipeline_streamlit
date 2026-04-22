"""Two-hop query client: ES for candidate retrieval, PG for full record display."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator
from functools import cached_property

from data_integration_pipeline.common.core.models.templates.base_models import BaseGoldSchemaRecord
from data_integration_pipeline.gold.core.bm25_fields import BM25_TEXT_FIELDS
from data_integration_pipeline.gold.io.elasticsearch_client import ElasticsearchClient
from data_integration_pipeline.gold.io.postgres_client import PostgresClient

TABLE_NAME = 'integrated_records'

EMBEDDING_FIELDS = [
    'embedding_global_vector',
    'embedding_description_long',
    'embedding_description_short',
    'embedding_product_services',
    'embedding_market_niches',
    'embedding_business_model',
]


class SearchFilters(BaseModel):
    """Single structured query intent — an LLM fills whichever fields are relevant.

    The ``QueryClient`` translates this into the appropriate ES query:
    text fields become ``multi_match`` or ``match`` clauses, keyword fields
    become ``term`` filters, numeric fields become ``range`` filters, and
    vectors become KNN queries.
    """

    k: int = Field(default=1, description='Number of results to return')
    # --- free text (multi_match must clause) ---
    query_text: Optional[str] = Field(
        default=None, description='Free-text search query matched against description, industry, products, niches, and business model'
    )
    query_text_fields: Optional[list[str]] = Field(
        default=None,
        description=f'BM25 target fields for query_text. Defaults to all text fields when omitted. Valid values: {", ".join(BM25_TEXT_FIELDS)}',
    )

    # --- semantic / vector search ---
    semantic_query: Optional[str] = Field(
        default=None,
        description='Text for semantic/vector search — gets embedded at query time via EmbeddingClient',
    )
    embedding_fields: Optional[list[str]] = Field(
        # if no particular fields are specified, use the global vector
        default=['embedding_global_vector'],
        description=f'Which embedding fields to search. Defaults to all fields if semantic_query is set. Valid values: {", ".join(EMBEDDING_FIELDS)}',
    )
    num_candidates: int = Field(
        default=100,
        description='HNSW candidate pool size for KNN search. Higher values improve recall at the cost of latency.',
    )

    # --- keyword filters (term, exact match) ---
    country: Optional[str] = Field(default=None, description='Country name filter (exact match, case-insensitive)')
    iso2_country_code: Optional[str] = Field(default=None, description='ISO-2 country code filter, e.g. "FI", "DE", "US"')
    company_id: Optional[str] = Field(default=None, description='Exact company ID filter')
    normalized_request_url: Optional[str] = Field(default=None, description='Exact normalized URL filter')

    # --- numeric range filters ---
    min_employees: Optional[int] = Field(default=None, description='Minimum number of employees (inclusive)')
    max_employees: Optional[int] = Field(default=None, description='Maximum number of employees (inclusive)')
    min_founded_year: Optional[int] = Field(default=None, description='Earliest founding year (inclusive)')
    max_founded_year: Optional[int] = Field(default=None, description='Latest founding year (inclusive)')
    min_revenue: Optional[int] = Field(default=None, description='Minimum revenue lower bound (inclusive)')
    max_revenue: Optional[int] = Field(default=None, description='Maximum revenue upper bound (inclusive)')

    @model_validator(mode='after')
    def _validate_embedding_fields(self) -> 'SearchFilters':
        if self.query_text_fields is not None:
            invalid = [f for f in self.query_text_fields if f not in BM25_TEXT_FIELDS]
            if invalid:
                raise ValueError(f'Invalid query_text_fields: {invalid}. Valid: {BM25_TEXT_FIELDS}')
        if self.embedding_fields is not None:
            invalid = [f for f in self.embedding_fields if f not in EMBEDDING_FIELDS]
            if invalid:
                raise ValueError(f'Invalid embedding fields: {invalid}. Valid: {EMBEDDING_FIELDS}')
        if self.embedding_fields or self.semantic_query:
            if not self.semantic_query or not self.embedding_fields:
                self.semantic_query = None
                self.embedding_fields = None
        return self

    def __bool__(self) -> bool:
        ignored_fields = {'k', 'num_candidates', 'embedding_fields', 'query_text_fields'}
        return any(v is not None for field_name, v in self.model_dump().items() if field_name not in ignored_fields)


class QueryClient:
    """Two-hop retrieval: ES narrows candidates by relevance, PG returns full records.

    Supports BM25 full-text search, semantic (KNN) vector search, and hybrid
    (BM25 + KNN) — all combinable with structured exact-match and range filters.

    Usage::

        client = QueryClient()

        # BM25 text search
        results = client.search(SearchFilters(
            query_text="fraud detection banking analytics",
            country="finland",
        ))

        # Semantic search across all embedding fields
        results = client.search(SearchFilters(
            semantic_query="AI-powered fraud detection for banking",
            country="finland",
        ))

        # Hybrid: BM25 + semantic
        results = client.search(SearchFilters(
            query_text="fraud detection",
            semantic_query="AI-powered fraud detection for banking",
        ))
    """

    def __init__(
        self,
        es_client: Optional[ElasticsearchClient] = None,
        pg_client: Optional[PostgresClient] = None,
        table_name: str = TABLE_NAME,
    ) -> None:
        self.es_client = es_client or ElasticsearchClient()
        self.pg_client = pg_client or PostgresClient()
        self.table_name = table_name
        self._embedding_client: Any = None

    @cached_property
    def embedding_client(self):
        """Lazy-loaded ``EmbeddingClient`` — model is only loaded on first semantic search."""
        from data_integration_pipeline.gold.core.embedding_client import EmbeddingClient

        return EmbeddingClient()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_filter_clauses(filters: SearchFilters) -> list[dict[str, Any]]:
        """Convert structured filter fields into a list of ES ``bool.filter`` clauses."""
        clauses: list[dict[str, Any]] = []

        # keyword (term) filters
        if filters.country is not None:
            clauses.append({'term': {'country': filters.country.lower()}})
        if filters.iso2_country_code is not None:
            clauses.append({'term': {'iso2_country_code': filters.iso2_country_code.lower()}})
        if filters.company_id is not None:
            clauses.append({'term': {'company_id': filters.company_id}})
        if filters.normalized_request_url is not None:
            clauses.append({'term': {'normalized_request_url': filters.normalized_request_url}})

        # numeric range filters
        emp_range: dict[str, int] = {}
        if filters.min_employees is not None:
            emp_range['gte'] = filters.min_employees
        if filters.max_employees is not None:
            emp_range['lte'] = filters.max_employees
        if emp_range:
            clauses.append({'range': {'employees_count': emp_range}})

        year_range: dict[str, int] = {}
        if filters.min_founded_year is not None:
            year_range['gte'] = filters.min_founded_year
        if filters.max_founded_year is not None:
            year_range['lte'] = filters.max_founded_year
        if year_range:
            clauses.append({'range': {'founded_year': year_range}})

        if filters.min_revenue is not None:
            clauses.append({'range': {'revenue_quantity_lower_bound': {'gte': filters.min_revenue}}})
        if filters.max_revenue is not None:
            clauses.append({'range': {'revenue_quantity_upper_bound': {'lte': filters.max_revenue}}})
        return clauses

    def _build_knn_clauses(
        self,
        query_vector: list[float],
        fields: list[str],
        k: int,
        num_candidates: int,
        filter_clauses: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build one ES KNN clause per embedding field, with optional pre-filtering."""
        knn_clauses: list[dict[str, Any]] = []
        for field in fields:
            clause: dict[str, Any] = {
                'field': field,
                'query_vector': query_vector,
                'k': k,
                'num_candidates': num_candidates,
            }
            if filter_clauses:
                clause['filter'] = filter_clauses
            knn_clauses.append(clause)
        return knn_clauses

    def _search_and_enrich(self, search_response: dict[str, Any], k: int) -> list[BaseGoldSchemaRecord]:
        """Extract ``hk`` IDs from an ES response, fetch full PG records in ES rank order."""
        hits = search_response.get('hits', {}).get('hits', [])
        if not hits:
            return []
        if len(hits) < k:
            k = len(hits)
        hks = [hit['_id'] for hit in hits[:k]]
        res = []
        for record in self.pg_client.get_records_by_hks(self.table_name, hks):
            res.append(record.gold_record)
        return res

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(self, filters: SearchFilters, size: int = 10) -> list[BaseGoldSchemaRecord]:
        """Execute a search from a single ``SearchFilters`` intent object.

        Automatically selects the right ES query strategy:
        - If ``query_text`` is set: BM25 full-text search.
        - If ``semantic_query`` is set: KNN vector search on selected embedding fields.
        - If both are set: hybrid search (ES merges BM25 + KNN scores).
        - If neither is set: filter-only search (match_all with filters).

        BM25 field scope can be controlled via ``query_text_fields``.
        All strategies can be combined with exact-match and range filters.
        All results are enriched with full PG records.
        """
        filter_clauses = self._build_filter_clauses(filters)
        index = self.es_client._resolve_index()

        search_kwargs: dict[str, Any] = {'index': index, 'size': size}

        # --- BM25 text path ---
        bool_query: dict[str, Any] = {}
        if filters.query_text is not None:
            bm25_fields = filters.query_text_fields
            if bm25_fields is None:
                bm25_fields = BM25_TEXT_FIELDS
            if bm25_fields:
                bool_query['must'] = {
                    'multi_match': {
                        'query': filters.query_text,
                        'fields': bm25_fields,
                    }
                }
        elif filters.semantic_query is None:
            bool_query['must'] = {'match_all': {}}

        if filter_clauses and not filters.semantic_query:
            bool_query['filter'] = filter_clauses

        if bool_query:
            search_kwargs['query'] = {'bool': bool_query}

        # --- Semantic / KNN path ---
        if filters.semantic_query is not None:
            query_vector = self.embedding_client.embed_query(filters.semantic_query)
            fields = filters.embedding_fields or EMBEDDING_FIELDS
            num_candidates = max(filters.num_candidates, size)
            knn_clauses = self._build_knn_clauses(
                query_vector=query_vector,
                fields=fields,
                k=size,
                num_candidates=num_candidates,
                filter_clauses=filter_clauses,
            )
            search_kwargs['knn'] = knn_clauses

        response = self.es_client.es_client.search(**search_kwargs)
        return self._search_and_enrich(response, k=filters.k)


if __name__ == '__main__':
    """
    Search & Retrieval Usecase:
    To help ground your design, below are example queries that reflect how the final system is
    expected to be used.
    Your pipeline should produce final datasets that make these types of queries efficient to
    execute.
    ● Find fintech companies in Finland working on fraud detection or banking analytics
    ● Show German healthcare companies focused on diagnostics and patient monitoring
    with more than 100 employees founded after 2022
    ● Biotech startups in France working on drug discovery with revenue above 100M
    ● Energy companies in Sweden building software for smart grids or renewable
    forecasting
    ● US technology companies building infrastructure for data pipelines and observability
    ● Show healthcare companies in Finland focused on diagnostics founded before 2005
    ● Find telecom companies in the UK working on 5G analytics with fewer than 20
    """
    query_client = QueryClient()

    # --- BM25 text search examples ---
    bm25_filters = [
        SearchFilters(query_text='Fintech fraud detection banking analytics', country='Finland'),
        SearchFilters(query_text='Healthcare diagnostics patient monitoring', country='Germany', min_employees=100, min_founded_year=2022),
        SearchFilters(query_text='Biotech drug discovery', country='France', min_revenue=100_000_000),
        SearchFilters(query_text='Energy smart grids renewable forecasting', country='Sweden'),
        SearchFilters(query_text='Technology data pipelines observability', country='US'),
        SearchFilters(query_text='Telecom 5G analytics data pipelines observability', country='UK'),
    ]

    # --- Semantic search examples ---
    semantic_filters = [
        SearchFilters(semantic_query='AI-powered fraud detection and banking analytics platform'),
        SearchFilters(semantic_query='medical diagnostics patient monitoring healthcare', country='germany', min_employees=100),
        SearchFilters(
            semantic_query='renewable energy smart grid forecasting',
            embedding_fields=['embedding_product_services', 'embedding_market_niches'],
        ),
        # Hybrid: BM25 + semantic
        SearchFilters(query_text='fraud detection', semantic_query='AI-powered fraud detection for banking', country='finland'),
    ]

    for search_filter in bm25_filters + semantic_filters:
        results = query_client.search(search_filter)
        print('-' * 100)
        print(f'search_filter: {search_filter}')
        print('-' * 100)
        for result in results:
            print(result)
            print('*' * 100)
