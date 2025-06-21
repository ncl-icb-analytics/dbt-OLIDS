-- Mart model for LTC LCS Case Finding CKD_61
-- Identifies patients with two consecutive eGFR readings below 60 (case finding for undiagnosed CKD)

{{ config(materialized='table') }}

select
    person_id,
    age,
    has_ckd,
    latest_egfr_date,
    previous_egfr_date,
    latest_egfr_value,
    previous_egfr_value,
    all_egfr_codes,
    all_egfr_displays,
    meets_criteria,
    current_timestamp() as last_updated
from {{ ref('int_ltc_lcs_cf_ckd_61') }}
where meets_criteria = true  -- Only include patients who meet the criteria
