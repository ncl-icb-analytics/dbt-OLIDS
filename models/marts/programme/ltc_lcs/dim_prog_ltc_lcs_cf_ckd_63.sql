-- Mart model for LTC LCS Case Finding CKD_63
-- Identifies patients with latest UACR reading above 70 (case finding for undiagnosed CKD)
-- Excludes patients already captured in CKD_62

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CKD_63 case finding dimension table for LTC/LCS programme. Identifies patients aged 17+ with latest UACR (urine albumin-to-creatinine ratio) reading above 70 mg/mmol, indicating severe proteinuria and possible undiagnosed chronic kidney disease. These patients are excluded from CKD_62 criteria and require urgent clinical assessment for CKD diagnosis and management. Used to prioritise patients for immediate nephrology review and proteinuria management.'"
) }}

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
