/*
Simplified Health and Social Care Worker Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following worker codes (latest occurrence):
   - Care home worker (CAREHOME_COD)
   - Nursing home worker (NURSEHOME_COD)  
   - Domiciliary care worker (DOMCARE_COD)
2. AND aged 16 years or older (minimum age for health/social care worker flu vaccination)

Combination rule - multiple worker categories with OR logic.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with care home worker codes (for all campaigns)
people_with_care_home_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_carehome_date,
        'Care home worker' AS worker_type,
        cc.audit_end_date
    FROM ({{ get_observations("'CAREHOME_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with nursing home worker codes (for all campaigns)
people_with_nursing_home_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_nursehome_date,
        'Nursing home worker' AS worker_type,
        cc.audit_end_date
    FROM ({{ get_observations("'NURSEHOME_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Find people with domiciliary care worker codes (for all campaigns)
people_with_domcare_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_domcare_date,
        'Domiciliary care worker' AS worker_type,
        cc.audit_end_date
    FROM ({{ get_observations("'DOMCARE_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 4: Combine all health and social care worker evidence (for all campaigns)
all_hcworker_evidence AS (
    SELECT campaign_id, person_id, latest_carehome_date AS evidence_date, worker_type
    FROM people_with_care_home_codes
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_nursehome_date, worker_type
    FROM people_with_nursing_home_codes
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_domcare_date, worker_type
    FROM people_with_domcare_codes
),

-- Step 5: Get the most recent evidence per person (for all campaigns)
best_hcworker_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        worker_type,
        evidence_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY evidence_date DESC) AS rn
    FROM all_hcworker_evidence
),

-- Step 6: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bhe.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Health and Social Care Workers' AS risk_group,
        bhe.person_id,
        bhe.evidence_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Health and social care workers aged 16-64' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_hcworker_evidence bhe
    JOIN all_campaigns cc
        ON bhe.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bhe.person_id = demo.person_id
    WHERE bhe.rn = 1  -- Only the most recent evidence per person
        -- Apply age restrictions: 16 to under 65 years (192 months to under 65 years)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 192
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id