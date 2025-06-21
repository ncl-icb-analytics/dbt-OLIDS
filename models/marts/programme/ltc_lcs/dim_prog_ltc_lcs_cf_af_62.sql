{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'AF_62 case finding dimension table for LTC/LCS programme. Identifies patients aged 65+ who are missing pulse checks in the last 36 months and may have undiagnosed atrial fibrillation. These patients require clinical assessment with pulse checks and ECG monitoring to screen for AF. Used to prioritise elderly patients for cardiovascular screening and AF case finding.'"
) }}

-- Dimension mart for LTC LCS Case Finding AF_62: Patients over 65 missing pulse check in last 36 months
-- Only includes patients who meet all criteria for AF_62

select
    person_id,
    age,
    has_pulse_check,
    latest_pulse_check_date,
    latest_health_check_date,
    has_excluding_condition,
    all_pulse_check_codes,
    all_pulse_check_displays
from {{ ref('int_ltc_lcs_cf_af_62') }}
where has_excluding_condition = false
  and (has_pulse_check = false or latest_pulse_check_date is null)
