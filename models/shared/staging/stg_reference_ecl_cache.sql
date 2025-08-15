-- Staging model for reference.ECL_CACHE
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CLUSTER_ID" as cluster_id,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "LAST_REFRESHED" as last_refreshed,
    "ECL_EXPRESSION_HASH" as ecl_expression_hash
from {{ source('reference', 'ECL_CACHE') }}
