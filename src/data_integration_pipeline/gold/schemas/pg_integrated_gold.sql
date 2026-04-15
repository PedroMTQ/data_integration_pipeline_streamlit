-- Integrated gold: one row per company from integrated silver Delta (Postgres serving layer).
-- Idempotent: IF NOT EXISTS on table and index.

CREATE TABLE IF NOT EXISTS integrated_records (
    hk uuid NOT NULL PRIMARY KEY,
    company_id text NOT NULL,
    hdiff text NOT NULL,
    country text,
    iso2_country_code text,
    request_url text,
    normalized_request_url text,
    employees_count integer,
    industry text,
    founded_year integer,
    revenue_quantity_lower_bound integer,
    revenue_quantity_upper_bound integer,
    revenue_unit_lower_bound text,
    revenue_unit_upper_bound text,
    description_long text,
    description_short text,
    product_services text[],
    market_niches text[],
    business_model text[],
    embedding_global_vector real[],
    embedding_description_long real[],
    embedding_description_short real[],
    embedding_product_services real[],
    embedding_market_niches real[],
    embedding_business_model real[],
    embedding_ldts timestamptz,
    load_ldts timestamptz NOT NULL,
    sync_ldts timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS integrated_records_normalized_request_url_idx ON integrated_records (normalized_request_url) WHERE normalized_request_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS integrated_records_company_id_idx ON integrated_records (company_id);
