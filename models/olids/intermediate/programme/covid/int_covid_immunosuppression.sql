/*
COVID Immunosuppression Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following evidence of immunosuppression:
   - Immunosuppression diagnosis (IMMDX_COV_COD) - latest occurrence
   - Immunosuppression medication (IMMRX_COD) since lookback date (6 months)
   - Immunosuppression administration (IMM_ADM_COD) since lookback date (3 years)  
   - Chemotherapy/radiotherapy (DXT_CHEMO_COD) since lookback date (6 months)
2. AND aged 6 months to 74 years (for 2025/26 campaigns) or any age (for 2024/25)

Combination rule - multiple evidence sources with OR logic.
KEY ELIGIBILITY GROUP for restricted 2025/26 campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with immunosuppression diagnosis (for all campaigns)
people_with_immuno_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_diagnosis_date,
        'Immunosuppression diagnosis' AS evidence_type
    FROM ({{ get_observations("'IMMDX_COV_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND cc.eligible_immunosuppression = TRUE
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with recent immunosuppression medications (for all campaigns)
people_with_recent_immuno_medications AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_medication_date,
        'Recent immunosuppression medication' AS evidence_type
    FROM ({{ get_medication_orders(cluster_id='IMMRX_COD', source='UKHSA_COVID') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.immuno_medication_lookback_date
        AND med.order_date <= cc.audit_end_date
        AND cc.eligible_immunosuppression = TRUE
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 3: Find people with recent immunosuppression administration codes (for all campaigns)
people_with_recent_immuno_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admin_date,
        'Recent immunosuppression administration' AS evidence_type
    FROM ({{ get_observations("'IMM_ADM_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_admin_lookback_date
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
    FROM ({{ get_observations("'DXT_CHEMO_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_medication_lookback_date  -- Same 6-month lookback
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 5: Union all evidence sources
all_immuno_evidence AS (
    SELECT 
        campaign_id, person_id, latest_diagnosis_date AS evidence_date, evidence_type
    FROM people_with_immuno_diagnosis
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_medication_date AS evidence_date, evidence_type
    FROM people_with_recent_immuno_medications
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_admin_date AS evidence_date, evidence_type
    FROM people_with_recent_immuno_admin
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_chemo_date AS evidence_date, evidence_type
    FROM people_with_recent_chemo
),

-- Step 6: Get people with any immunosuppression evidence
people_with_immunosuppression AS (
    SELECT 
        aie.campaign_id,
        aie.person_id,
        MAX(aie.evidence_date) AS latest_evidence_date,
        LISTAGG(DISTINCT aie.evidence_type, '; ') AS evidence_types,
        cc.campaign_reference_date,
        cc.audit_end_date
    FROM all_immuno_evidence aie
    LEFT JOIN all_campaigns cc ON aie.campaign_id = cc.campaign_id
    GROUP BY 
        aie.campaign_id, aie.person_id, cc.campaign_reference_date, 
        cc.audit_end_date
),

-- Step 7: Add age information and apply campaign-specific age restrictions
people_immunosuppressed_with_age AS (
    SELECT 
        pwi.campaign_id,
        pwi.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pwi.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pwi.campaign_reference_date) AS age_months_at_ref_date,
        pwi.latest_evidence_date,
        pwi.evidence_types,
        pwi.campaign_reference_date
    FROM people_with_immunosuppression pwi
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pwi.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND DATEDIFF('month', demo.birth_date_approx, pwi.campaign_reference_date) >= 6  -- Minimum age 6 months
),

-- Step 8: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Immunosuppression' AS risk_group,
        person_id,
        latest_evidence_date AS qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Immunosuppressed patient: ', evidence_types) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_immunosuppressed_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id