-- Staging model for CODESETS.PCD_REFSET_LATEST
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "CLUSTER_ID" AS cluster_id,
    "CLUSTER_DESCRIPTION" AS cluster_description,
    "SNOMED_CODE" AS snomed_code,
    "SNOMED_CODE_DESCRIPTION" AS snomed_code_description,
    "PCD_REFSET_ID" AS pcd_refset_id,
    "SERVICE_AND_RULESET" AS service_and_ruleset
FROM {{ source('CODESETS', 'PCD_REFSET_LATEST') }}
