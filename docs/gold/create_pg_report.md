# create_pg_report

## Purpose

`CreatePostgresReportJob` creates (or replaces) a PostgreSQL summary view called `integrated_records_report` over the `integrated_records` table. This single-row view provides at-a-glance data quality metrics: record counts, non-null coverage for seed and joined data fields, uniqueness indicators, and revenue consistency checks. It serves as a lightweight dashboard for operators and reviewers.

## Entrypoint

| Method | Command |
|---|---|
| CLI | `dip pg-report` |


## Report content

**Record counts:**
- `total_records`: Total row count.
- `distinct_company_ids`: Number of unique company IDs.
- `distinct_urls`: Number of unique normalized URLs.

**Dataset 1 (seed) coverage** -- count of non-null values for key fields:
- `d1_has_country`, `d1_has_iso2_country_code`, `d1_has_normalized_request_url`
- `d1_has_employees_count`, `d1_has_founded_year`, `d1_has_industry`, `d1_has_description_short`

**Dataset 2 coverage** -- count of non-null values for fields sourced from dataset 2:
- `d2_has_description_long`, `d2_has_product_services`, `d2_has_market_niches`, `d2_has_business_model`

**Dataset 3 coverage** -- count of non-null embedding fields sourced from dataset 3:
- `d3_has_emb_description_long`, `d3_has_emb_description_short`, etc.

**Uniqueness checks:**
- `duplicate_company_ids`: `total - distinct` (should be 0 for correct data).
- `duplicate_urls`: Same logic for URLs.

**Revenue consistency:**
- `revenue_bounds_inconsistent`: Count of rows where `lower_bound > upper_bound`.
