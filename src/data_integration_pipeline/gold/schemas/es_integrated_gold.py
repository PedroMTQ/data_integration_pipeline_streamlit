from data_integration_pipeline.settings import EMBEDDING_DIMENSIONS, ELASTICSEARCH_NUMBER_OF_SHARDS

_DENSE_VECTOR = {'type': 'dense_vector', 'dims': EMBEDDING_DIMENSIONS, 'index': True, 'similarity': 'cosine'}


def _TEXT_KW(ignore_above=256) -> dict:
    return {
        'type': 'text',
        'fields': {'keyword': {'type': 'keyword', 'normalizer': 'lowercase_ascii', 'ignore_above': ignore_above}},
    }


_INTEGRATED_GOLD_INDEX_TEMPLATE: dict = {
    'settings': {
        'number_of_shards': ELASTICSEARCH_NUMBER_OF_SHARDS,
        'analysis': {
            'normalizer': {
                'lowercase_ascii': {'type': 'custom', 'filter': ['lowercase', 'asciifolding']},
            }
        },
    },
    'mappings': {
        'properties': {
            # --- identifiers ---
            'hk': {'type': 'keyword'},
            'company_id': {'type': 'keyword'},
            # --- text search ---
            'industry': _TEXT_KW(150),
            'description_short': {'type': 'text'},
            'description_long': {'type': 'text'},
            # --- keyword / filter ---
            'country': {'type': 'keyword', 'normalizer': 'lowercase_ascii', 'ignore_above': 256},
            'iso2_country_code': {'type': 'keyword', 'normalizer': 'lowercase_ascii', 'ignore_above': 5},
            'normalized_request_url': {'type': 'keyword'},
            # --- arrays (text + keyword) ---
            'product_services': _TEXT_KW(),
            'market_niches': _TEXT_KW(),
            'business_model': _TEXT_KW(),
            # --- faceted / numeric ---
            'employees_count': {'type': 'integer'},
            'founded_year': {'type': 'integer'},
            'revenue_quantity_lower_bound': {'type': 'long'},
            'revenue_quantity_upper_bound': {'type': 'long'},
            # --- dense vectors ---
            'embedding_global_vector': _DENSE_VECTOR,
            'embedding_description_long': _DENSE_VECTOR,
            'embedding_description_short': _DENSE_VECTOR,
            'embedding_product_services': _DENSE_VECTOR,
            'embedding_market_niches': _DENSE_VECTOR,
            'embedding_business_model': _DENSE_VECTOR,
        }
    },
}
