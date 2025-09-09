/*
COVID Chronic Neurological Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. Chronic neurological disease diagnosis (CNS_COV_COD) - any time in history
2. AND aged 5+ years (minimum age for COVID vaccination)
3. Only eligible in 2024/25 campaigns (broader eligibility)

Simple diagnosis rule - any CNS diagnosis qualifies.
Includes stroke, TIA, cerebral palsy, MS, epilepsy, significant learning disorders.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with chronic neurological disease diagnosis (for all campaigns)
people_with_cns_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_cns_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'CNS_COV_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_chronic_neurological_disease = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date,
        cc.campaign_reference_date
),

-- Step 2: Add age information and apply age restrictions  
people_with_cns_eligible_with_age AS (
    SELECT 
        pcns.campaign_id,
        pcns.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pcns.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pcns.campaign_reference_date) AS age_months_at_ref_date,
        pcns.first_cns_date AS qualifying_event_date,
        pcns.campaign_reference_date
    FROM people_with_cns_diagnosis pcns
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pcns.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND DATEDIFF('year', demo.birth_date_approx, pcns.campaign_reference_date) >= 5  -- Minimum age 5
),

-- Step 3: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Chronic Neurological Disease' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Chronic neurological disease diagnosis' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_cns_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id