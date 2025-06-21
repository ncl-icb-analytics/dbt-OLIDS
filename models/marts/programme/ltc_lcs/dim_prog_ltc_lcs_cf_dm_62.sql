{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS Case Finding DM_62: Patients with gestational diabetes and pregnancy risk who have no HbA1c reading in the last 12 months. These patients require urgent diabetes screening due to their high-risk status. Excludes patients on LTC registers or with NHS health check in last 24 months. Used for targeted diabetes screening programmes.'"
    )
}}

-- Mart model for LTC LCS Case Finding DM_62
-- Patients with gestational diabetes risk requiring urgent HbA1c screening

SELECT
    person_id,
    age,
    has_gestational_diabetes_risk,
    latest_hba1c_date,
    latest_hba1c_value,
    all_gestational_diabetes_codes,
    all_gestational_diabetes_displays
FROM {{ ref('int_ltc_lcs_cf_dm_62') }} 