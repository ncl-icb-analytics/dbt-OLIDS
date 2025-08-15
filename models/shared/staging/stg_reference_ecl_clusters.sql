-- Staging model for reference.ECL_CLUSTERS
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CLUSTER_ID" as cluster_id,
    "ECL_EXPRESSION" as ecl_expression,
    "DESCRIPTION" as description,
    "CREATED_AT" as created_at,
    "UPDATED_AT" as updated_at,
    "CREATED_BY" as created_by,
    "UPDATED_BY" as updated_by,
    "CLUSTER_TYPE" as cluster_type
from {{ source('reference', 'ECL_CLUSTERS') }}
