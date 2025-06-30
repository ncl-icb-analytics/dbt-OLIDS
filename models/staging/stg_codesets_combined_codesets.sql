-- Staging model for CODESETS.COMBINED_CODESETS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "CLUSTER_ID" AS cluster_id,
    "CLUSTER_DESCRIPTION" AS cluster_description,
    "CODE" AS code,
    "CODE_DESCRIPTION" AS code_description,
    "SOURCE" AS source
FROM {{ source('CODESETS', 'COMBINED_CODESETS') }}
