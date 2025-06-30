-- Staging model for RULESETS.IMMS_SCHEDULE_LATEST
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".RULESETS

SELECT
    "VACCINE_ORDER" AS vaccine_order,
    "VACCINE_ID" AS vaccine_id,
    "VACCINE_NAME" AS vaccine_name,
    "DOSE_NUMBER" AS dose_number,
    "DISEASES_PROTECTED_AGAINST" AS diseases_protected_against,
    "VACCINE_CODE" AS vaccine_code,
    "TRADE_NAME" AS trade_name,
    "ADMINISTRATION_ROUTE" AS administration_route,
    "SCHEDULE_AGE" AS schedule_age,
    "MINIMUM_AGE_DAYS" AS minimum_age_days,
    "MAXIMUM_AGE_DAYS" AS maximum_age_days,
    "MINIMUM_INTERVAL_DAYS" AS minimum_interval_days,
    "NEXT_DOSE_VACCINE_ID" AS next_dose_vaccine_id,
    "ELIGIBLE_AGE_FROM_DAYS" AS eligible_age_from_days,
    "ELIGIBLE_AGE_TO_DAYS" AS eligible_age_to_days,
    "ADMINISTERED_CLUSTER_ID" AS administered_cluster_id,
    "DRUG_CLUSTER_ID" AS drug_cluster_id,
    "DECLINED_CLUSTER_ID" AS declined_cluster_id,
    "CONTRAINDICATED_CLUSTER_ID" AS contraindicated_cluster_id,
    "INCOMPATIBLE_CLUSTER_IDS" AS incompatible_cluster_ids,
    "INELIGIBILITY_PERIOD_MONTHS" AS ineligibility_period_months,
    "INCOMPATIBLE_EXPLANATION" AS incompatible_explanation
FROM {{ source('RULESETS', 'IMMS_SCHEDULE_LATEST') }}
