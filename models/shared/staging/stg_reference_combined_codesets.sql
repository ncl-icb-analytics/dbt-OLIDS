-- Staging model for reference.COMBINED_CODESETS
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "CODE" as code,
    "CODE_DESCRIPTION" as code_description,
    "SOURCE" as source
from {{ source('reference', 'COMBINED_CODESETS') }}
