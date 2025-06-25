-- Staging model for CODESETS.VALPROATE_PROG_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "CODE" AS code,
    "CODE_CATEGORY" AS code_category,
    "LOOKBACK_YEARS_OFFSET" AS lookback_years_offset,
    "VALPROATE_PRODUCT_TERM" AS valproate_product_term
FROM {{ source('CODESETS', 'VALPROATE_PROG_CODES') }}
