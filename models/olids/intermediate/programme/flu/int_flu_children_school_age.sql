/*
Children School Age Eligibility Rule

Business Rule: Person is eligible if they are:
1. Born between the campaign-specific date range for school age children
   (age range varies by campaign year - typically 4-16 years old)
   (dates determined by campaign configuration child_school_age_birth_start/end)

Pure birth date range rule - no clinical codes, just demographics.
Age-agnostic naming allows for year-to-year age range changes.
Typically covers Reception to Year 11 but can be adjusted per campaign.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find children in the school age range based on birth dates (for all campaigns)
children_school_age AS (
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
    WHERE demo.birth_date_approx BETWEEN cc.child_school_age_birth_start AND cc.child_school_age_birth_end
),

-- Step 2: Format as standard eligibility output (for all campaigns)
final_eligibility AS (
    SELECT 
        csa.campaign_id,
        'Age-Based' AS campaign_category,
        'Children School Age' AS risk_group,
        csa.person_id,
        NULL AS qualifying_event_date,  -- No specific event for age-based rules
        csa.campaign_reference_date AS reference_date,
        'Children of school age (campaign-specific birth date range)' AS description,
        csa.birth_date_approx,
        csa.age_months_at_ref_date,
        csa.age_years_at_ref_date,
        csa.audit_end_date AS created_at
    FROM children_school_age csa
)

SELECT * FROM final_eligibility
ORDER BY person_id