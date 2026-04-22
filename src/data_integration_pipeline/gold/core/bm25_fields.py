"""Shared BM25 text-field metadata for query logic and UI labels."""

BM25_TEXT_FIELD_SPECS: list[tuple[str, str]] = [
    ('description_short^2', 'Short Description (boosted)'),
    ('description_long', 'Business Description'),
    ('industry', 'Industry'),
    ('product_services', 'Products & Services'),
    ('market_niches^2', 'Market Niches (boosted)'),
    ('business_model^2', 'Business Model (boosted)'),
]

BM25_TEXT_FIELDS: list[str] = [field for field, _ in BM25_TEXT_FIELD_SPECS]
BM25_TEXT_FIELD_LABELS: dict[str, str] = dict(BM25_TEXT_FIELD_SPECS)
