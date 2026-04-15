from typing import Any, Iterable
import urllib3
import warnings
from data_integration_pipeline.common.core.models.integrated.es_schema import ElasticsearchSchemaRecord
from data_integration_pipeline.gold.schemas.es_integrated_gold import _INTEGRATED_GOLD_INDEX_TEMPLATE
from data_integration_pipeline.settings import (
    ES_MAX_RETRIES,
    ES_RETRY_BACKOFF_SECONDS,
    ES_RETRY_MAX_BACKOFF_SECONDS,
    ELASTICSEARCH_INDEX_ALIAS,
    ELASTICSEARCH_INDEX_NAME,
    ELASTICSEARCH_PASSWORD,
    ELASTICSEARCH_URL,
    ELASTICSEARCH_USER,
    ES_CLIENT_BATCH_SIZE,
    ES_CLIENT_RAISE_ERRORS,
)
from data_integration_pipeline.common.io.logger import logger

# this disables the warning about insecure requests, we keep it only for dev/poc
warnings.filterwarnings('ignore', category=Warning, module='elasticsearch')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from elasticsearch import Elasticsearch, NotFoundError  # noqa: E402
from elasticsearch.helpers import streaming_bulk  # noqa: E402


class ElasticsearchClient:
    """Elasticsearch client aligned with ``PostgresClient``: settings from env unless overridden in ``__init__``."""

    def __init__(
        self,
        urls: list[str] | None = None,
        user: str | None = None,
        password: str | None = None,
        index_name: str | None = None,
        index_alias: str | None = None,
        # execution settings
        batch_size: int = ES_CLIENT_BATCH_SIZE,
        raise_errors: bool = ES_CLIENT_RAISE_ERRORS,
        # retry settings
        max_retries: int = ES_MAX_RETRIES,
        retry_backoff_seconds: float = ES_RETRY_BACKOFF_SECONDS,
        retry_max_backoff_seconds: float = ES_RETRY_MAX_BACKOFF_SECONDS,
    ) -> None:
        self.__urls = urls if urls is not None else [ELASTICSEARCH_URL]
        self.__user = user if user is not None else ELASTICSEARCH_USER
        self.__password = password if password is not None else ELASTICSEARCH_PASSWORD
        self.__index_name = index_name if index_name is not None else ELASTICSEARCH_INDEX_NAME
        self.__index_alias = index_alias if index_alias is not None else ELASTICSEARCH_INDEX_ALIAS
        if not self.__urls:
            raise ValueError('Set ELASTICSEARCH_URLS or ELASTICSEARCH_URL')
        auth = None
        if self.__user and self.__password:
            auth = (self.__user, self.__password)
        # TODO add cert path and verify certs. this is easier for now in dev and also for airflow deployment. but is not secure.
        self.es_client = Elasticsearch(self.__urls, basic_auth=auth, verify_certs=False)
        self.batch_size = batch_size
        self.raise_errors = raise_errors

        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.retry_max_backoff_seconds = retry_max_backoff_seconds

    @staticmethod
    def _to_document(source: dict[str, Any], hit_id: str | None = None) -> ElasticsearchSchemaRecord:
        """Build a typed document from an Elasticsearch ``_source`` payload."""
        try:
            return ElasticsearchSchemaRecord(**source)
        except Exception as e:
            doc_ref = hit_id if hit_id is not None else source.get('hk')
            raise ValueError(f'Failed to parse Elasticsearch document {doc_ref}: {e}') from e

    def _extract_documents(self, search_response: dict[str, Any]) -> list[ElasticsearchSchemaRecord]:
        """Convert an Elasticsearch ``search`` response into typed documents."""
        documents: list[ElasticsearchSchemaRecord] = []
        for hit in search_response.get('hits', {}).get('hits', []):
            source = hit.get('_source') or {}
            documents.append(self._to_document(source=source, hit_id=hit.get('_id')))
        return documents

    def _resolve_index(self, index: str | None = None) -> str:
        """Resolve explicit index if provided, otherwise default to configured alias."""
        return index if index else self.__index_alias

    def __str__(self) -> str:
        return '\n'.join(
            [
                'ElasticsearchClient:',
                f'\tUrls:\t{self.__urls}',
                f'\tUser:\t{self.__user}',
                f'\tIndex name:\t{self.__index_name}',
                f'\tIndex alias:\t{self.__index_alias}',
                f'\tBatch size:\t{self.batch_size}',
                f'\tRaise errors:\t{self.raise_errors}',
                f'\tMax retries:\t{self.max_retries}',
                f'\tRetry backoff (s):\t{self.retry_backoff_seconds}',
                f'\tRetry max backoff (s):\t{self.retry_max_backoff_seconds}',
            ]
        )

    @staticmethod
    def get_index_settings_and_mappings() -> tuple[dict[str, Any], dict[str, Any]]:
        """Return (settings, mappings) for indices.create()."""
        settings = dict(_INTEGRATED_GOLD_INDEX_TEMPLATE['settings'])
        mappings = _INTEGRATED_GOLD_INDEX_TEMPLATE['mappings']
        return settings, mappings

    def init_index(self) -> None:
        """Create integrated gold index (if missing) and ensure read alias exists (idempotent)."""
        if self.es_client.indices.exists(index=self.__index_name):
            logger.debug(f'Index {self.__index_name} already exists; ensuring alias {self.__index_alias} if missing')
        else:
            settings, mappings = ElasticsearchClient.get_index_settings_and_mappings()
            self.es_client.indices.create(index=self.__index_name, settings=settings, mappings=mappings)
            logger.info(f'Created index {self.__index_name} with mapping')

        if not self.es_client.indices.exists_alias(name=self.__index_alias):
            self.es_client.indices.put_alias(index=self.__index_name, name=self.__index_alias)
            logger.info(f'Added alias {self.__index_alias} -> {self.__index_name}')
        else:
            logger.debug(f'Alias {self.__index_alias} already exists')

    def count_all(self, index: str | None = None) -> int:
        """Return total document count for an index/alias.

        Args:
            index: Optional explicit index/alias override. When omitted, uses the
                configured default alias.

        Returns:
            Total number of indexed documents.

        Use when:
            Running quick sanity checks after indexing runs or local data resets.
        """
        resolved_index = self._resolve_index(index)
        count_response = self.es_client.count(index=resolved_index, query={'match_all': {}})
        return int(count_response.get('count', 0))

    def get_document_by_id(self, doc_id: str, index: str | None = None) -> ElasticsearchSchemaRecord | None:
        """Return one typed document by ES ``_id``, or ``None`` if missing."""
        resolved_index = self._resolve_index(index)
        try:
            get_response = self.es_client.get(index=resolved_index, id=doc_id)
        except NotFoundError:
            return None
        source_payload = get_response.get('_source') or {}
        return self._to_document(source=source_payload, hit_id=get_response.get('_id'))

    def search_knn(
        self,
        query_vector: list[float],
        vector_field: str,
        k: int = 10,
        num_candidates: int = 100,
        index: str | None = None,
    ) -> list[ElasticsearchSchemaRecord]:
        resolved_index = self._resolve_index(index)
        knn_query = {
            'field': vector_field,
            'query_vector': query_vector,
            # k represents the number of results to return
            'k': k,
            # num_candidates represents the number of candidates to consider before final top-k scoring
            'num_candidates': num_candidates,
        }
        search_response = self.es_client.search(
            index=resolved_index,
            knn={**knn_query},
            size=k,
        )
        return self._extract_documents(search_response)

    def search_text_similarity(
        self,
        query_text: str,
        fields: list[str] | None = None,
        size: int = 10,
        min_score: float | None = None,
        index: str | None = None,
    ) -> list[ElasticsearchSchemaRecord]:
        """Return documents ranked by BM25 text relevance."""
        resolved_index = self._resolve_index(index)
        query_fields = (
            fields
            if fields
            else [
                'description_short^2',
                'description_long',
                'industry',
                'product_services',
                'market_niches',
                'business_model',
            ]
        )
        search_request: dict[str, Any] = {
            'index': resolved_index,
            'query': {
                'multi_match': {
                    'query': query_text,
                    'fields': query_fields,
                }
            },
            'size': size,
        }
        if min_score is not None:
            search_request['min_score'] = min_score
        search_response = self.es_client.search(**search_request)
        return self._extract_documents(search_response)

    def search_keyword(
        self,
        field: str,
        keyword: str,
        k: int = 10,
        index: str | None = None,
        exact: bool = True,
    ) -> list[ElasticsearchSchemaRecord]:
        """Return documents matching a specific field by exact or analyzed keyword search."""
        resolved_index = self._resolve_index(index)
        if exact:
            # For exact matching we target the keyword subfield convention when available.
            exact_field = field if field.endswith('.keyword') else f'{field}.keyword'
            search_query = {'term': {exact_field: keyword}}
        else:
            search_query = {'match': {field: keyword}}
        search_response = self.es_client.search(index=resolved_index, query=search_query, size=k)
        return self._extract_documents(search_response)

    def _build_actions(self, data: Iterable[dict]):
        """Yield bulk-index action dicts, skipping documents without an ``hk``."""
        for doc in data:
            doc_id = doc.get('hk')
            if doc_id:
                yield {'_op_type': 'index', '_index': self.__index_alias, '_id': str(doc_id), '_source': doc}

    def ping(self) -> bool:
        """Ping Elasticsearch server."""
        try:
            if not self.es_client.ping():
                raise Exception(f'Failed to ping Elasticsearch at {self.__urls}')
        except Exception as e:
            logger.critical(f'Failed to ping Elasticsearch at {self.__urls}, exception: {e}')
            raise e
        return True

    def load_stream(self, data_stream: Iterable[dict]):
        for is_success, item in streaming_bulk(
            client=self.es_client,
            actions=self._build_actions(data_stream),
            chunk_size=self.batch_size,
            raise_on_error=self.raise_errors,
            raise_on_exception=self.raise_errors,
            max_retries=self.max_retries,
            initial_backoff=self.retry_backoff_seconds,
            max_backoff=self.retry_max_backoff_seconds,
            yield_ok=True,
        ):
            yield is_success, item


if __name__ == '__main__':
    client = ElasticsearchClient()
    print(client.ping())
    # doc = client.get_document_by_id('1262ff9e-4863-574e-a1c6-419a30dc01ea')
    # print(doc)
    # search_result = client.search_text_similarity(query_text='healthcare')
    # for doc in search_result:
    #     print(doc)
    search_result = client.search_keyword(field='industry', keyword='healthcare', size=1, exact=True)
    print(search_result)
    test_vector = [
        -0.5906,
        -0.7504,
        0.9718,
        -0.5199,
        0.5677,
        0.9987,
        0.1614,
        0.5895,
        0.0938,
        -0.5521,
        0.7517,
        -0.2552,
        0.5376,
        -0.3142,
        -0.4589,
        0.1781,
        -0.7788,
        0.8141,
        -0.9087,
        -0.6808,
        0.3072,
        0.7157,
        -0.163,
        0.4613,
        -0.0856,
        0.0489,
        -0.0168,
        0.7347,
        0.5682,
        0.5075,
        -0.8993,
        0.3079,
        -0.7656,
        -0.3304,
        -0.4868,
        -0.5298,
        -0.8919,
        0.5653,
        -0.3576,
        0.7731,
        0.4118,
        -0.1706,
        0.0299,
        -0.5824,
        -0.8449,
        0.7204,
        -0.447,
        0.2277,
        -0.4351,
        -0.241,
        -0.3683,
        0.2181,
        0.4095,
        0.9259,
        0.1788,
        0.5911,
        -0.512,
        0.1487,
        0.2454,
        0.6515,
        0.4784,
        -0.891,
        -0.609,
        -0.0653,
        -0.5573,
        0.5305,
        -0.8406,
        0.7236,
        -0.7107,
        0.3051,
        -0.7332,
        -0.3296,
        -0.9698,
        -0.3153,
        -0.42,
        -0.5748,
        -0.7416,
        0.4548,
        0.3936,
        -0.0534,
        0.466,
        -0.5951,
        -0.978,
        -0.536,
        -0.7257,
        -0.4382,
        -0.2016,
        -0.2992,
        -0.3458,
        0.4885,
        0.2019,
        -0.9133,
        -0.6893,
        0.0887,
        0.1428,
        0.3477,
        -0.9289,
        -0.9177,
        -0.5971,
        0.6977,
        -0.5403,
        0.4101,
        0.4239,
        -0.0455,
        0.5729,
        -0.6672,
        -0.5906,
        -0.632,
        -0.2589,
        -0.1721,
        -0.7479,
        0.6173,
        0.8198,
        -0.624,
        -0.3109,
        -0.8185,
        -0.5119,
        0.3048,
        0.5403,
        0.989,
        0.8869,
        -0.1003,
        -0.8516,
        0.041,
        -0.5684,
        -0.0053,
        -0.1741,
        -0.8336,
    ]
    search_result = client.search_knn(query_vector=test_vector, vector_field='embedding_description_long', k=1, num_candidates=10)
    print(search_result)
    # for doc in search_result:
    #     print(doc)
    # print(client.search_text_similarity(query_text='texas', fields=['province'], size=1))
