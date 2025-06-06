-- Staging model for RULESETS.BP_THRESHOLDS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".RULESETS

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
from {{ source('RULESETS', 'BP_THRESHOLDS') }}
