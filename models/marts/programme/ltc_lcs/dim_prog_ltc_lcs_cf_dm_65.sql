{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS Case Finding DM_65: Patients with moderate-high BMI based on ethnicity (27.5-32.5 for BAME, 30-35 for non-BAME) who have not had HbA1c testing in the last 24 months. These patients have moderate diabetes risk and require screening. Excludes patients on LTC registers or with NHS health check in last 24 months. Used for diabetes prevention programmes targeting moderate-risk populations.'"
    )
}}

-- Mart model for LTC LCS Case Finding DM_65
-- Patients with moderate-high BMI requiring diabetes screening

SELECT
    person_id,
    age,
    has_moderate_high_bmi,
    is_bame,
    latest_bmi_date,
    latest_bmi_value,
    latest_hba1c_date,
    latest_hba1c_value,
    all_bmi_codes,
    all_bmi_displays
FROM {{ ref('int_ltc_lcs_cf_dm_65') }} 