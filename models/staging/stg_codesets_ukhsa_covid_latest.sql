-- Staging model for CODESETS.UKHSA_COVID_LATEST
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "CODING_SCHEME" AS coding_scheme,
    "LIBRARY" AS library,
    "CLUSTER_ID" AS cluster_id,
    "CLUSTER_DESCRIPTION" AS cluster_description,
    "SNOMED_CODE" AS snomed_code,
    "SNOMED_DESCRIPTION" AS snomed_description,
    "CODE_VALIDATED" AS code_validated
FROM {{ source('REFERENCE', 'UKHSA_COVID_LATEST') }}
