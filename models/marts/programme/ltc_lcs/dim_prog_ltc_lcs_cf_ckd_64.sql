-- Mart model for LTC LCS Case Finding CKD_64
-- Identifies patients with specific conditions requiring eGFR monitoring
-- (AKI, BPH/Gout, Lithium, Microhaematuria) who have not had eGFR in last 12 months

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CKD_64 case finding dimension table for LTC/LCS programme. Identifies patients aged 17+ with specific conditions requiring eGFR monitoring (acute kidney injury, BPH/gout, lithium medication, or microhaematuria) who have not had eGFR testing in the last 12 months. These patients require kidney function monitoring due to their risk factors for chronic kidney disease. Used to prioritise patients for eGFR testing and nephrology assessment.'"
) }}

select
    person_id,
    age,
    has_acute_kidney_injury,
    has_bph_gout,
    has_lithium_medication,
    has_microhaematuria,
    latest_aki_date,
    latest_bph_gout_date,
    latest_lithium_date,
    latest_microhaematuria_date,
    latest_uacr_date,
    latest_uacr_value,
    all_condition_codes,
    all_condition_displays,
    meets_criteria,
    current_timestamp() as last_updated
from {{ ref('int_ltc_lcs_cf_ckd_64') }}
where meets_criteria = true  -- Only include patients who meet the criteria
