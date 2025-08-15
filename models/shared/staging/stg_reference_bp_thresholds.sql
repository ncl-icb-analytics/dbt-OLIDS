-- Staging model for reference.BP_THRESHOLDS
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "THRESHOLD_RULE_ID" as threshold_rule_id,
    "PROGRAMME_OR_GUIDELINE" as programme_or_guideline,
    "DESCRIPTION" as description,
    "PATIENT_GROUP" as patient_group,
    "THRESHOLD_TYPE" as threshold_type,
    "SYSTOLIC_THRESHOLD" as systolic_threshold,
    "DIASTOLIC_THRESHOLD" as diastolic_threshold,
    "OPERATOR" as operator,
    "NOTES" as notes
from {{ source('reference', 'BP_THRESHOLDS') }}
