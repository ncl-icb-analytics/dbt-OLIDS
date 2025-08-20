/*
Simplified Chronic Respiratory Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following respiratory conditions:
   - Eligible via Active Asthma Management (AST_GROUP)
   - Eligible via Asthma Admission (AST_ADM_GROUP)  
   - Chronic respiratory disease diagnosis (RESP_COD) - earliest occurrence
2. AND aged 6 months or older (minimum age for flu vaccination)

Combination rule - combines existing asthma eligibility with additional respiratory codes.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Get people eligible via active asthma management (for all campaigns)
people_eligible_via_asthma AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date,
        'Eligible via active asthma management' AS eligibility_reason
    FROM {{ ref('int_flu_active_asthma_management') }}
),

-- Step 2: Get people eligible via asthma admission (for all campaigns)
people_eligible_via_asthma_admission AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date,
        'Eligible via asthma admission' AS eligibility_reason
    FROM {{ ref('int_flu_asthma_admission') }}
),

-- Step 3: Find people with chronic respiratory disease diagnosis (for all campaigns)
people_with_chronic_resp_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_resp_date,
        'Chronic respiratory disease diagnosis' AS eligibility_reason,
        cc.audit_end_date
    FROM ({{ get_observations("'RESP_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 4: Combine all respiratory eligibility paths (for all campaigns)
all_respiratory_eligibility AS (
    SELECT campaign_id, person_id, qualifying_event_date, eligibility_reason
    FROM people_eligible_via_asthma
    
    UNION
    
    SELECT campaign_id, person_id, qualifying_event_date, eligibility_reason
    FROM people_eligible_via_asthma_admission
    
    UNION
    
    SELECT campaign_id, person_id, first_resp_date, eligibility_reason
    FROM people_with_chronic_resp_diagnosis
),

-- Step 5: Remove duplicates and get best qualifying event per person (for all campaigns)
best_respiratory_eligibility AS (
    SELECT 
        campaign_id,
        person_id,
        eligibility_reason,
        qualifying_event_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_respiratory_eligibility
),

-- Step 6: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bre.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Chronic Respiratory Disease' AS risk_group,
        bre.person_id,
        bre.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with chronic lung conditions (asthma, COPD, cystic fibrosis, etc.)' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_respiratory_eligibility bre
    JOIN all_campaigns cc
        ON bre.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bre.person_id = demo.person_id
    WHERE bre.rn = 1  -- Only the best eligibility per person
        -- Apply age restrictions: 6 months or older (minimum age for flu vaccination)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id