-- Staging model for CODESETS.ETHNICITY_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "CODE" AS code,
    "TERM" AS term,
    "CATEGORY" AS category,
    "SUBCATEGORY" AS subcategory,
    "GRANULAR" AS granular
FROM {{ source('CODESETS', 'ETHNICITY_CODES') }}
