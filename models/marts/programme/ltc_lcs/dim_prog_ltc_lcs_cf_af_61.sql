{{ config(
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'AF_61 case finding dimension table for LTC/LCS programme. Identifies patients on specific cardiac medications (digoxin, flecainide, propafenone, or anticoagulants) who might have undiagnosed atrial fibrillation. These patients require clinical assessment to confirm or rule out AF diagnosis and ensure appropriate management. Used to prioritise patients for ECG monitoring and cardiology review.'"
) }}

-- Mart model for LTC LCS Case Finding: AF_61
-- Patients on digoxin, flecainide, propafenone or anticoagulants who might have undiagnosed AF.

SELECT DISTINCT
    bp.person_id,
    ms.has_active_anticoagulant,
    ms.has_active_digoxin,
    ms.has_active_cardiac_glycoside,
    ms.latest_af_medication_date,
    ms.latest_health_check_date,
    COALESCE(ms.has_exclusion_condition, FALSE) AS has_exclusion_condition,
    ms.exclusion_reason,
    ms.all_af_medication_codes,
    ms.all_af_medication_displays
FROM {{ ref('int_ltc_lcs_cf_base_population') }} bp
LEFT JOIN {{ ref('int_ltc_lcs_cf_af_61') }} ms
    ON bp.person_id = ms.person_id
-- Note: latest_health_check_date and has_recent_health_check_24m can be added when a health check intermediate is available

