/*
COVID Learning Disability Eligibility Rule

Business Rule: Person is eligible if they have:
1. Learning disability diagnosis (LD_COD) - any time in history
2. AND aged 5+ years (minimum age for COVID vaccination)  
3. Eligible in all campaigns (universal eligibility)

Simple diagnosis rule - any learning disability diagnosis qualifies.
This condition is eligible in both 2024/25 AND 2025/26 campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with learning disability diagnosis (for all campaigns)
people_with_ld_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_ld_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'LEARNDIS_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND cc.eligible_learning_disability = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 2: Add age information and apply age restrictions  
people_with_ld_eligible_with_age AS (
    SELECT 
        pld.campaign_id,
        pld.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pld.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pld.campaign_reference_date) AS age_months_at_ref_date,
        pld.first_ld_date AS qualifying_event_date,
        pld.campaign_reference_date
    FROM people_with_ld_diagnosis pld
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pld.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND DATEDIFF('year', demo.birth_date_approx, pld.campaign_reference_date) >= 5  -- Minimum age 5
),

-- Step 3: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Learning Disability' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Learning disability diagnosis (all campaigns)' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_ld_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id