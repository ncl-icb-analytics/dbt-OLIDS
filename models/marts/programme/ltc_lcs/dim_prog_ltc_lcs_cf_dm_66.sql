{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS Case Finding DM_66: Patients with recent HbA1c readings between 42-46 mmol/mol within the last 12 months. These patients have borderline elevated glucose levels requiring monitoring and lifestyle interventions. Excludes patients on LTC registers or with NHS health check in last 24 months. Used for diabetes prevention and early intervention programmes.'"
    )
}}

-- Mart model for LTC LCS Case Finding DM_66
-- Patients with borderline HbA1c requiring intervention

SELECT
    person_id,
    age,
    has_elevated_hba1c,
    latest_hba1c_date,
    latest_hba1c_value,
    all_hba1c_codes,
    all_hba1c_displays
FROM {{ ref('int_ltc_lcs_cf_dm_66') }} 