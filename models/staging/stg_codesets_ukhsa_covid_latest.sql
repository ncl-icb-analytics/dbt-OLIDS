-- Staging model for CODESETS.UKHSA_COVID_LATEST
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "CODING_SCHEME" as coding_scheme,
    "LIBRARY" as library,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "CODE_VALIDATED" as code_validated
from {{ source('CODESETS', 'UKHSA_COVID_LATEST') }}
