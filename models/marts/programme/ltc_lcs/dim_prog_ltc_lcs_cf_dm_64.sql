{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS Case Finding DM_64: Patients with ethnicity-based high BMI (≥32.5 for BAME, ≥35 for non-BAME) who have not had HbA1c testing in the last 24 months. These patients require urgent diabetes screening due to their elevated BMI and lack of recent monitoring. Excludes patients on LTC registers or with NHS health check in last 24 months. Used for targeted diabetes screening based on obesity risk factors.'"
    )
}}

-- Mart model for LTC LCS Case Finding DM_64
-- Patients with high BMI requiring diabetes screening based on ethnicity thresholds

SELECT
    person_id,
    age,
    has_high_bmi,
    is_bame,
    latest_bmi_date,
    latest_bmi_value,
    latest_hba1c_date,
    latest_hba1c_value,
    all_bmi_codes,
    all_bmi_displays
FROM {{ ref('int_ltc_lcs_cf_dm_64') }} 