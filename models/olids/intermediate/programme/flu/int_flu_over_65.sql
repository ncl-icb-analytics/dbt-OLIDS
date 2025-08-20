/*
Simplified Over 65 Age-Based Eligibility Rule

Business Rule: Person is eligible if they are:
1. Aged 65 years or over at the campaign reference date

This is the simplest possible rule - pure age-based eligibility.
No clinical codes, no complex logic, just age calculation.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people aged 65+ at reference date (for all campaigns)
people_over_65 AS (
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
    WHERE demo.birth_date_approx <= DATEADD('year', -65, cc.campaign_reference_date)
),

-- Step 2: Format as standard eligibility output (for all campaigns)
final_eligibility AS (
    SELECT 
        p65.campaign_id,
        'Age-Based' AS campaign_category,
        'Age 65 and Over' AS risk_group,
        p65.person_id,
        NULL AS qualifying_event_date,  -- No specific event for age-based rules
        p65.campaign_reference_date AS reference_date,
        'Everyone aged 65 and over at campaign reference date' AS description,
        p65.birth_date_approx,
        p65.age_months_at_ref_date,
        p65.age_years_at_ref_date,
        p65.audit_end_date AS created_at
    FROM people_over_65 p65
)

SELECT * FROM final_eligibility
ORDER BY person_id