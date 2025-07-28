/*
Simplified Health and Social Care Worker Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following worker codes (latest occurrence):
   - Care home worker (CAREHOME_COD)
   - Nursing home worker (NURSEHOME_COD)  
   - Domiciliary care worker (DOMCARE_COD)
2. AND aged 16 to under 65 years (192 months to under 65 years)

Combination rule - multiple worker categories with OR logic.
*/

{{ config(materialized='table') }}

{%- set campaign_id = var('flu_current_campaign', 'flu_2024_25') -%}

WITH campaign_config AS (
    {{ flu_campaign_config(campaign_id) }}
),

-- Step 1: Find people with care home worker codes
people_with_care_home_codes AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_carehome_date,
        'Care home worker' AS worker_type
    FROM ({{ get_observations("'CAREHOME_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 2: Find people with nursing home worker codes
people_with_nursing_home_codes AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_nursehome_date,
        'Nursing home worker' AS worker_type
    FROM ({{ get_observations("'NURSEHOME_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 3: Find people with domiciliary care worker codes
people_with_domcare_codes AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_domcare_date,
        'Domiciliary care worker' AS worker_type
    FROM ({{ get_observations("'DOMCARE_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 4: Combine all health and social care worker evidence
all_hcworker_evidence AS (
    SELECT person_id, latest_carehome_date AS evidence_date, worker_type
    FROM people_with_care_home_codes
    
    UNION ALL
    
    SELECT person_id, latest_nursehome_date, worker_type
    FROM people_with_nursing_home_codes
    
    UNION ALL
    
    SELECT person_id, latest_domcare_date, worker_type
    FROM people_with_domcare_codes
),

-- Step 5: Get the most recent evidence per person
best_hcworker_evidence AS (
    SELECT 
        person_id,
        worker_type,
        evidence_date,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY evidence_date DESC) AS rn
    FROM all_hcworker_evidence
),

-- Step 6: Add demographics and apply age restrictions
final_eligibility AS (
    SELECT 
        '{{ campaign_id }}' AS campaign_id,
        'HCWORKER_GROUP' AS rule_group_id,
        'Health and Social Care Workers' AS rule_group_name,
        bhe.person_id,
        bhe.evidence_date AS qualifying_event_date,
        bhe.worker_type,
        cc.campaign_reference_date AS reference_date,
        'Health and social care workers aged 16-64' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_hcworker_evidence bhe
    CROSS JOIN campaign_config cc
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bhe.person_id = demo.person_id
    WHERE bhe.rn = 1  -- Only the most recent evidence per person
        -- Apply age restrictions: 16 to under 65 years (192 months to under 65 years)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 192
        AND DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) < 65
)

SELECT * FROM final_eligibility
ORDER BY person_id