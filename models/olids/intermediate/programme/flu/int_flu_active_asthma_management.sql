/*
Simplified Asthma Eligibility Rule

Business Rule: Person is eligible if they have:
1. An asthma diagnosis (AST_COD) - any time in history
2. AND recent evidence of active asthma management:
   - Asthma medication prescription (ASTRX_COD) since lookback date, OR
   - Asthma medication administration (ASTMED_COD) since lookback date, OR  
   - Asthma hospital admission (ASTADM_COD) - any time in history
3. AND aged 6 months to under 65 years

This replaces the complex macro-based approach with clear, readable SQL.
*/

{{ config(materialized='table') }}

{%- set campaign_id = var('flu_current_campaign', 'flu_2024_25') -%}

WITH campaign_config AS (
    {{ flu_campaign_config(campaign_id) }}
),

-- Step 1: Find people with asthma diagnosis
people_with_asthma_diagnosis AS (
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS first_asthma_date
    FROM ({{ get_observations("'AST_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 2: Find people with recent asthma medications (prescriptions)
people_with_recent_asthma_prescriptions AS (
    SELECT 
        person_id,
        MAX(order_date) AS latest_prescription_date
    FROM ({{ get_medication_orders(cluster_id='ASTRX_COD', source='UKHSA_FLU') }})
    CROSS JOIN campaign_config cc
    WHERE order_date IS NOT NULL
        AND order_date >= cc.asthma_medication_lookback_date
        AND order_date <= cc.audit_end_date
    GROUP BY person_id
),

-- Step 3: Find people with recent asthma medication administration
people_with_recent_asthma_medications AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_medication_date
    FROM ({{ get_observations("'ASTMED_COD'", 'UKHSA_FLU') }})
    CROSS JOIN campaign_config cc
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date >= cc.asthma_medication_lookback_date
        AND clinical_effective_date <= cc.audit_end_date
    GROUP BY person_id
),

-- Step 4: Find people with asthma hospital admissions (any time)
people_with_asthma_admissions AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_admission_date
    FROM ({{ get_observations("'ASTADM_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 5: Combine all evidence of active asthma management
people_with_active_asthma_evidence AS (
    SELECT DISTINCT person_id, 'Recent prescription' AS evidence_type, latest_prescription_date AS evidence_date
    FROM people_with_recent_asthma_prescriptions
    
    UNION ALL
    
    SELECT DISTINCT person_id, 'Recent medication', latest_medication_date
    FROM people_with_recent_asthma_medications
    
    UNION ALL
    
    SELECT DISTINCT person_id, 'Hospital admission', latest_admission_date
    FROM people_with_asthma_admissions
),

-- Step 6: Get the most recent evidence per person
best_asthma_evidence AS (
    SELECT 
        person_id,
        evidence_type,
        evidence_date,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY evidence_date DESC) AS rn
    FROM people_with_active_asthma_evidence
),

-- Step 7: Combine diagnosis with evidence requirement
asthma_eligible_people AS (
    SELECT 
        diag.person_id,
        diag.first_asthma_date,
        evid.evidence_type,
        evid.evidence_date
    FROM people_with_asthma_diagnosis diag
    INNER JOIN best_asthma_evidence evid
        ON diag.person_id = evid.person_id
        AND evid.rn = 1  -- Most recent evidence only
),

-- Step 8: Add demographics and apply age restrictions
final_eligibility AS (
    SELECT 
        '{{ campaign_id }}' AS campaign_id,
        'AST_GROUP' AS rule_group_id,
        'Active Asthma Management' AS rule_group_name,
        ae.person_id,
        ae.first_asthma_date AS qualifying_event_date,
        ae.evidence_date,
        ae.evidence_type,
        cc.campaign_reference_date AS reference_date,
        'People with asthma diagnosis and recent medication or admission' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM asthma_eligible_people ae
    CROSS JOIN campaign_config cc
    JOIN {{ ref('dim_person_demographics') }} demo
        ON ae.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months to under 65 years
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
        AND DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) < 65
)

SELECT * FROM final_eligibility
ORDER BY person_id