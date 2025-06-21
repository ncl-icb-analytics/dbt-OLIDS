-- Mart model for LTC LCS Case Finding CKD_62
-- Identifies patients with two consecutive UACR readings above 4 (case finding for undiagnosed CKD)

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CKD_62 case finding dimension table for LTC/LCS programme. Identifies patients aged 17+ with two consecutive UACR (urine albumin-to-creatinine ratio) readings above 4 mg/mmol, indicating significant proteinuria and possible undiagnosed chronic kidney disease. These patients require clinical assessment for CKD diagnosis and management. Used to prioritise patients for nephrology review and proteinuria investigation.'"
) }}

select
    person_id,
    age,
    has_elevated_uacr,
    latest_uacr_date,
    previous_uacr_date,
    latest_uacr_value,
    previous_uacr_value,
    all_uacr_codes,
    all_uacr_displays,
    meets_criteria,
    current_timestamp() as last_updated
from {{ ref('int_ltc_lcs_cf_ckd_62') }}
where meets_criteria = true  -- Only include patients who meet the criteria
