-- Mart model for LTC LCS Case Finding CKD_63
-- Identifies patients with latest UACR reading above 70 (case finding for undiagnosed CKD)
-- Excludes patients already captured in CKD_62

{{ config(materialized='table') }}

select
    person_id,
    age,
    has_elevated_uacr,
    latest_uacr_date,
    latest_uacr_value,
    all_uacr_codes,
    all_uacr_displays,
    meets_criteria,
    current_timestamp() as last_updated
from {{ ref('int_ltc_lcs_cf_ckd_63') }}
where meets_criteria = true  -- Only include patients who meet the criteria
