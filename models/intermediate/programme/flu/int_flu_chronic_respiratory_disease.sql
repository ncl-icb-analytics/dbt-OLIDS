/*
Simplified Chronic Respiratory Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following respiratory conditions:
   - Eligible via Active Asthma Management (AST_GROUP)
   - Eligible via Asthma Admission (AST_ADM_GROUP)  
   - Chronic respiratory disease diagnosis (RESP_COD) - earliest occurrence
2. AND aged 6 months to under 65 years

Combination rule - combines existing asthma eligibility with additional respiratory codes.
*/

{{ config(materialized='table') }}

{%- set campaign_id = var('flu_current_campaign', 'flu_2024_25') -%}

WITH campaign_config AS (
    {{ flu_campaign_config(campaign_id) }}
),

-- Step 1: Get people eligible via active asthma management
people_eligible_via_asthma AS (
    SELECT 
        person_id,
        qualifying_event_date,
        'Eligible via active asthma management' AS eligibility_reason
    FROM {{ ref('int_flu_active_asthma_management') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Step 2: Get people eligible via asthma admission
people_eligible_via_asthma_admission AS (
    SELECT 
        person_id,
        qualifying_event_date,
        'Eligible via asthma admission' AS eligibility_reason
    FROM {{ ref('int_flu_asthma_admission') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Step 3: Find people with chronic respiratory disease diagnosis
people_with_chronic_resp_diagnosis AS (
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS first_resp_date,
        'Chronic respiratory disease diagnosis' AS eligibility_reason
    FROM ({{ get_observations("'RESP_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 4: Combine all respiratory eligibility paths
all_respiratory_eligibility AS (
    SELECT person_id, qualifying_event_date, eligibility_reason
    FROM people_eligible_via_asthma
    
    UNION
    
    SELECT person_id, qualifying_event_date, eligibility_reason
    FROM people_eligible_via_asthma_admission
    
    UNION
    
    SELECT person_id, first_resp_date, eligibility_reason
    FROM people_with_chronic_resp_diagnosis
),

-- Step 5: Remove duplicates and get best qualifying event per person
best_respiratory_eligibility AS (
    SELECT 
        person_id,
        eligibility_reason,
        qualifying_event_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_respiratory_eligibility
),

-- Step 6: Add demographics and apply age restrictions
final_eligibility AS (
    SELECT 
        '{{ campaign_id }}' AS campaign_id,
        'RESP_GROUP' AS rule_group_id,
        'Chronic Respiratory Disease' AS rule_group_name,
        bre.person_id,
        bre.qualifying_event_date,
        bre.eligibility_reason,
        cc.campaign_reference_date AS reference_date,
        'People with chronic lung conditions (asthma, COPD, cystic fibrosis, etc.)' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_respiratory_eligibility bre
    CROSS JOIN campaign_config cc
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bre.person_id = demo.person_id
    WHERE bre.rn = 1  -- Only the best eligibility per person
        -- Apply age restrictions: 6 months to under 65 years
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
        AND DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) < 65
)

SELECT * FROM final_eligibility
ORDER BY person_id