{{
    config(
        materialized='table',
        post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS Case Finding DM_61: Patients at risk of diabetes who meet ANY of the following criteria: (1) HbA1c ≥ 42 mmol/mol within last 5 years, (2) QDiabetes score ≥ 5.6%, (3) QRisk2 score > 20%, or (4) History of gestational diabetes. Excludes patients on LTC registers or with NHS health check in last 24 months. Used for diabetes prevention and early intervention programmes.'"
    )
}}

-- Mart model for LTC LCS Case Finding DM_61
-- Patients at risk of diabetes based on clinical risk factors

SELECT
    person_id,
    age,
    has_diabetes_risk,
    latest_hba1c_date,
    latest_hba1c_value,
    latest_qdiabetes_date,
    latest_qdiabetes_value,
    latest_qrisk_date,
    latest_qrisk_value,
    has_gestational_diabetes,
    all_hba1c_codes,
    all_hba1c_displays
FROM {{ ref('int_ltc_lcs_cf_dm_61') }} 