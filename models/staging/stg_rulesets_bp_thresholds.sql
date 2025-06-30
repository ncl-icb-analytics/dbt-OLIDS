-- Staging model for RULESETS.BP_THRESHOLDS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".RULESETS

SELECT
    "THRESHOLD_RULE_ID" AS threshold_rule_id,
    "PROGRAMME_OR_GUIDELINE" AS programme_or_guideline,
    "DESCRIPTION" AS description,
    "PATIENT_GROUP" AS patient_group,
    "THRESHOLD_TYPE" AS threshold_type,
    "SYSTOLIC_THRESHOLD" AS systolic_threshold,
    "DIASTOLIC_THRESHOLD" AS diastolic_threshold,
    "OPERATOR" AS operator,
    "NOTES" AS notes
FROM {{ source('RULESETS', 'BP_THRESHOLDS') }}
