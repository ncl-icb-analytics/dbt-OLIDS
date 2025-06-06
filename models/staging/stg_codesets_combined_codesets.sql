-- Staging model for CODESETS.COMBINED_CODESETS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "CODE" as code,
    "CODE_DESCRIPTION" as code_description,
    "SOURCE" as source
from {{ source('CODESETS', 'COMBINED_CODESETS') }}
