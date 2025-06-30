-- Staging model for CODESETS.LTC_LCS_CODES
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "CLUSTER_ID" AS cluster_id,
    "CLUSTER_DESCRIPTION" AS cluster_description,
    "SNOMED_CODE" AS snomed_code,
    "SNOMED_DESCRIPTION" AS snomed_description
FROM {{ source('REFERENCE', 'LTC_LCS_CODES') }}
