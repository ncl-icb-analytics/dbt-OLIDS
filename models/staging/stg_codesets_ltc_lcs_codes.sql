-- Staging model for CODESETS.LTC_LCS_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description
from {{ source('CODESETS', 'LTC_LCS_CODES') }}
