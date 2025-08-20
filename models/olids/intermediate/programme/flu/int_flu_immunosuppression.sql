/*
Simplified Immunosuppression Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following evidence of immunosuppression:
   - Immunosuppression diagnosis (IMMDX_COD) - latest occurrence
   - Immunosuppression medication (IMMRX_COD) since lookback date
   - Immunosuppression administration (IMMADM_COD) since lookback date  
   - Chemotherapy/radiotherapy (DXT_CHEMO_COD) since lookback date
2. AND aged 6 months or older (minimum age for flu vaccination)

Combination rule - multiple evidence sources with OR logic.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with immunosuppression diagnosis (for all campaigns)
people_with_immuno_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_diagnosis_date,
        'Immunosuppression diagnosis' AS evidence_type
    FROM ({{ get_observations("'IMMDX_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with recent immunosuppression medications (for all campaigns)
people_with_recent_immuno_medications AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_medication_date,
        'Recent immunosuppression medication' AS evidence_type
    FROM ({{ get_medication_orders(cluster_id='IMMRX_COD', source='UKHSA_FLU') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.immuno_medication_lookback_date
        AND med.order_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 3: Find people with recent immunosuppression administration codes (for all campaigns)
people_with_recent_immuno_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admin_date,
        'Recent immunosuppression administration' AS evidence_type
    FROM ({{ get_observations("'IMMADM_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_medication_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 4: Find people with recent chemotherapy/radiotherapy (for all campaigns)
people_with_recent_chemo AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_chemo_date,
        'Recent chemotherapy/radiotherapy' AS evidence_type
    FROM ({{ get_observations("'DXT_CHEMO_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_medication_lookback_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 5: Combine all immunosuppression evidence (for all campaigns)
all_immuno_evidence AS (
    SELECT campaign_id, person_id, latest_diagnosis_date AS evidence_date, evidence_type
    FROM people_with_immuno_diagnosis
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_medication_date, evidence_type
    FROM people_with_recent_immuno_medications
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_admin_date, evidence_type
    FROM people_with_recent_immuno_admin
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_chemo_date, evidence_type
    FROM people_with_recent_chemo
),

-- Step 6: Get the most recent evidence per person per campaign
best_immuno_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        evidence_type,
        evidence_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY evidence_date DESC) AS rn
    FROM all_immuno_evidence
),

-- Step 7: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bie.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Immunosuppression' AS risk_group,
        bie.person_id,
        bie.evidence_date AS qualifying_event_date,
        bie.evidence_type,
        cc.campaign_reference_date AS reference_date,
        'People with weakened immune systems or receiving immunosuppressive treatment' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_immuno_evidence bie
    JOIN all_campaigns cc ON bie.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bie.person_id = demo.person_id
    WHERE bie.rn = 1  -- Only the most recent evidence per person per campaign
        -- Apply age restrictions: 6 months to under 65 years
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY person_id