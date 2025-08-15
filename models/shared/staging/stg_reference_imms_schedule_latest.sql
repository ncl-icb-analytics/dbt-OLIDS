-- Staging model for reference.IMMS_SCHEDULE_LATEST
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "VACCINE_ORDER" as vaccine_order,
    "VACCINE_ID" as vaccine_id,
    "VACCINE_NAME" as vaccine_name,
    "DOSE_NUMBER" as dose_number,
    "DISEASES_PROTECTED_AGAINST" as diseases_protected_against,
    "VACCINE_CODE" as vaccine_code,
    "TRADE_NAME" as trade_name,
    "ADMINISTRATION_ROUTE" as administration_route,
    "SCHEDULE_AGE" as schedule_age,
    "MINIMUM_AGE_DAYS" as minimum_age_days,
    "MAXIMUM_AGE_DAYS" as maximum_age_days,
    "MINIMUM_INTERVAL_DAYS" as minimum_interval_days,
    "NEXT_DOSE_VACCINE_ID" as next_dose_vaccine_id,
    "ELIGIBLE_AGE_FROM_DAYS" as eligible_age_from_days,
    "ELIGIBLE_AGE_TO_DAYS" as eligible_age_to_days,
    "ADMINISTERED_CLUSTER_ID" as administered_cluster_id,
    "DRUG_CLUSTER_ID" as drug_cluster_id,
    "DECLINED_CLUSTER_ID" as declined_cluster_id,
    "CONTRAINDICATED_CLUSTER_ID" as contraindicated_cluster_id,
    "INCOMPATIBLE_CLUSTER_IDS" as incompatible_cluster_ids,
    "INELIGIBILITY_PERIOD_MONTHS" as ineligibility_period_months,
    "INCOMPATIBLE_EXPLANATION" as incompatible_explanation
from {{ source('reference', 'IMMS_SCHEDULE_LATEST') }}
