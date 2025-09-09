/*
COVID Age 75+ Universal Eligibility Rule

Business Rule: Person is eligible if they are:
1. Aged 75 years or over at the campaign reference date

This is universal eligibility across all COVID campaigns (2024-2026).
The simplest possible rule - pure age-based eligibility.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people aged 75+ at reference date (for all campaigns)
people_age_75_plus AS (
    SELECT 
        cc.campaign_id,
        demo.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        cc.campaign_reference_date,
        cc.audit_end_date
    FROM {{ ref('dim_person_demographics') }} demo
    CROSS JOIN all_campaigns cc
    WHERE cc.eligible_age_75_plus = TRUE
        AND demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) >= 75
),

-- Step 2: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'AGE_BASED' AS campaign_category,
        'Age 75+' AS risk_group,
        person_id,
        campaign_reference_date AS qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Aged 75 years or over at campaign reference date' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_age_75_plus
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id