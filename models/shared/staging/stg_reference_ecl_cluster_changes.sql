-- Staging model for reference.ECL_CLUSTER_CHANGES
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CHANGE_ID" as change_id,
    "CLUSTER_ID" as cluster_id,
    "CHANGE_TYPE" as change_type,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "CHANGE_TIMESTAMP" as change_timestamp,
    "CHANGED_BY" as changed_by,
    "REFRESH_SESSION_ID" as refresh_session_id
from {{ source('reference', 'ECL_CLUSTER_CHANGES') }}
