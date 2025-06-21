-- Mart model for LTC LCS Case Finding CKD_62
-- Identifies patients with two consecutive UACR readings above 4 (case finding for undiagnosed CKD)

{{ config(materialized='table') }}

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
