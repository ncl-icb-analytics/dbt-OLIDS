-- Mart model for LTC LCS Case Finding CKD_61
-- Identifies patients with two consecutive eGFR readings below 60 (case finding for undiagnosed CKD)

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CKD_61 case finding dimension table for LTC/LCS programme. Identifies patients aged 17+ with two consecutive eGFR readings below 60 mL/min/1.73m², indicating possible undiagnosed chronic kidney disease (CKD stage 3 or worse). These patients require clinical assessment for CKD diagnosis, staging, and management initiation. Used to prioritise patients for nephrology review and CKD care pathway entry.'"
) }}

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
