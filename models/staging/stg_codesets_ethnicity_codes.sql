-- Staging model for CODESETS.ETHNICITY_CODES
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "CODE" AS code,
    "TERM" AS term,
    "CATEGORY" AS category,
    "SUBCATEGORY" AS subcategory,
    "GRANULAR" AS granular
FROM {{ source('REFERENCE', 'ETHNICITY_CODES') }}
