-- Staging model for reference.UKHSA_COVID_LATEST
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CODING_SCHEME" as coding_scheme,
    "LIBRARY" as library,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "CODE_VALIDATED" as code_validated
from {{ source('reference', 'UKHSA_COVID_LATEST') }}
