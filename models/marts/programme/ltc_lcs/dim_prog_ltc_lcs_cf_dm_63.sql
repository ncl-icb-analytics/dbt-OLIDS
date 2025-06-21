{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS Case Finding DM_63: Patients with elevated HbA1c (46-47 mmol/mol) who have not had HbA1c monitoring in the last 12 months. These patients are at increased risk of developing diabetes and require follow-up testing. Excludes patients on LTC registers or with NHS health check in last 24 months. Used for diabetes prevention monitoring programmes.'"
    )
}}

-- Mart model for LTC LCS Case Finding DM_63
-- Patients with elevated HbA1c requiring monitoring follow-up

SELECT
    person_id,
    age,
    has_elevated_hba1c,
    latest_hba1c_date,
    latest_hba1c_value,
    all_hba1c_codes,
    all_hba1c_displays
FROM {{ ref('int_ltc_lcs_cf_dm_63') }} 