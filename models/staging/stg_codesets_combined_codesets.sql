-- Staging model for CODESETS.COMBINED_CODESETS
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "CLUSTER_ID" AS cluster_id,
    "CLUSTER_DESCRIPTION" AS cluster_description,
    "CODE" AS code,
    "CODE_DESCRIPTION" AS code_description,
    "SOURCE" AS source
FROM {{ source('REFERENCE', 'COMBINED_CODESETS') }}
