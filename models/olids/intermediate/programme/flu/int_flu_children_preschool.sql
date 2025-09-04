/*
Children Preschool Age Eligibility Rule

Business Rule: Person is eligible if they are:
1. Born between the campaign-specific date range for preschool children
   (age range varies by campaign year - typically 2-3 years old)
   (dates determined by campaign configuration child_preschool_birth_start/end)

Pure birth date range rule - no clinical codes, just demographics.
Age-agnostic naming allows for year-to-year age range changes.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find children in the preschool age range based on birth dates (for all campaigns)
children_preschool AS (
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
    WHERE demo.birth_date_approx BETWEEN cc.child_preschool_birth_start AND cc.child_preschool_birth_end
),

-- Step 2: Format as standard eligibility output (for all campaigns)
final_eligibility AS (
    SELECT 
        cp.campaign_id,
        'Age-Based' AS campaign_category,
        'Children Preschool' AS risk_group,
        cp.person_id,
        NULL AS qualifying_event_date,  -- No specific event for age-based rules
        cp.campaign_reference_date AS reference_date,
        'Children of preschool age (campaign-specific birth date range)' AS description,
        cp.birth_date_approx,
        cp.age_months_at_ref_date,
        cp.age_years_at_ref_date,
        cp.audit_end_date AS created_at
    FROM children_preschool cp
)

SELECT * FROM final_eligibility
ORDER BY person_id