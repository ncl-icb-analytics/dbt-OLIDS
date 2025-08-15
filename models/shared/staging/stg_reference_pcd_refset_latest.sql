-- Staging model for reference.PCD_REFSET_LATEST
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_CODE_DESCRIPTION" as snomed_code_description,
    "PCD_REFSET_ID" as pcd_refset_id,
    "SERVICE_AND_RULESET" as service_and_ruleset
from {{ source('reference', 'PCD_REFSET_LATEST') }}
