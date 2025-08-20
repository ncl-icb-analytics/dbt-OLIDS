/*
Simplified Asthma Eligibility Rule

Business Rule: Person is eligible if they have:
1. An asthma diagnosis (AST_COD) - any time in history
2. AND recent evidence of active asthma management:
   - Asthma medication prescription (ASTRX_COD) since lookback date, OR
   - Asthma medication administration (ASTMED_COD) since lookback date, OR  
   - Asthma hospital admission (ASTADM_COD) - any time in history
3. AND aged 6 months or older (minimum age for flu vaccination)

This replaces the complex macro-based approach with clear, readable SQL.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with asthma diagnosis (for all campaigns)
people_with_asthma_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_asthma_date,
        cc.audit_end_date
    FROM ({{ get_observations("'AST_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with recent asthma medications (prescriptions, for all campaigns)
people_with_recent_asthma_prescriptions AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_prescription_date,
        cc.audit_end_date
    FROM ({{ get_medication_orders(cluster_id='ASTRX_COD', source='UKHSA_FLU') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.asthma_medication_lookback_date
        AND med.order_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, med.person_id, cc.audit_end_date
),

-- Step 3: Find people with recent asthma medication administration (for all campaigns)
people_with_recent_asthma_medications AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_medication_date,
        cc.audit_end_date
    FROM ({{ get_observations("'ASTMED_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.asthma_medication_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 4: Find people with asthma hospital admissions (any time, for all campaigns)
people_with_asthma_admissions AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admission_date,
        cc.audit_end_date
    FROM ({{ get_observations("'ASTADM_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 5: Combine all evidence of active asthma management (for all campaigns)
people_with_active_asthma_evidence AS (
    SELECT DISTINCT campaign_id, person_id, 'Recent prescription' AS evidence_type, latest_prescription_date AS evidence_date
    FROM people_with_recent_asthma_prescriptions
    
    UNION ALL
    
    SELECT DISTINCT campaign_id, person_id, 'Recent medication', latest_medication_date
    FROM people_with_recent_asthma_medications
    
    UNION ALL
    
    SELECT DISTINCT campaign_id, person_id, 'Hospital admission', latest_admission_date
    FROM people_with_asthma_admissions
),

-- Step 6: Get the most recent evidence per person (for all campaigns)
best_asthma_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        evidence_type,
        evidence_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY evidence_date DESC) AS rn
    FROM people_with_active_asthma_evidence
),

-- Step 7: Combine diagnosis with evidence requirement (for all campaigns)
asthma_eligible_people AS (
    SELECT 
        diag.campaign_id,
        diag.person_id,
        diag.first_asthma_date,
        evid.evidence_type,
        evid.evidence_date
    FROM people_with_asthma_diagnosis diag
    INNER JOIN best_asthma_evidence evid
        ON diag.campaign_id = evid.campaign_id
        AND diag.person_id = evid.person_id
        AND evid.rn = 1  -- Most recent evidence only
),

-- Step 8: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        ae.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Active Asthma Management' AS risk_group,
        ae.person_id,
        ae.first_asthma_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with asthma diagnosis and recent medication or admission' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM asthma_eligible_people ae
    JOIN all_campaigns cc
        ON ae.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON ae.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months or older (minimum age for flu vaccination)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id