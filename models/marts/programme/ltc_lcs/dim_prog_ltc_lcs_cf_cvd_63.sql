-- Mart model for LTC LCS Case Finding CVD_63
-- Identifies patients on statins with non-HDL cholesterol > 2.5 (statin review needed)

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_63 case finding dimension table for LTC/LCS programme. Identifies patients currently on statin medications with non-HDL cholesterol levels > 2.5 mmol/L, indicating suboptimal lipid control despite treatment. These patients require statin therapy review and optimisation to achieve target cholesterol levels. Used to prioritise patients for medication review and lipid management optimisation.'"
) }}

SELECT
    person_id,
    age,
    needs_statin_review,
    latest_statin_date,
    latest_non_hdl_date,
    latest_non_hdl_value,
    all_statin_codes,
    all_statin_displays,
    all_non_hdl_codes,
    all_non_hdl_displays,
    meets_criteria,
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ ref('int_ltc_lcs_cf_cvd_63') }}
WHERE meets_criteria = TRUE  -- Only include patients who meet the criteria 