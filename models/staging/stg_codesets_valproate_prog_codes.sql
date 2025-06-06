-- Staging model for CODESETS.VALPROATE_PROG_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "CODE" as code,
    "CODE_CATEGORY" as code_category,
    "LOOKBACK_YEARS_OFFSET" as lookback_years_offset,
    "VALPROATE_PRODUCT_TERM" as valproate_product_term
from {{ source('CODESETS', 'VALPROATE_PROG_CODES') }}
