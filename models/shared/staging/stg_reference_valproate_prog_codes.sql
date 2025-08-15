-- Staging model for reference.VALPROATE_PROG_CODES
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CODE" as code,
    "CODE_CATEGORY" as code_category,
    "LOOKBACK_YEARS_OFFSET" as lookback_years_offset,
    "VALPROATE_PRODUCT_TERM" as valproate_product_term
from {{ source('reference', 'VALPROATE_PROG_CODES') }}
