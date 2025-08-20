/*
Simplified Severe Obesity (BMI) Eligibility Rule

Business Rule: Person is eligible if they have:
1. ANY of the following evidence of severe obesity:
   - BMI value >= 40 (from BMI_COD observations)
   - Severe obesity diagnosis code (SEV_OBESITY_COD)
2. AND aged 18 years or older (minimum age for obesity flu vaccination)

Hierarchical rule - BMI values and diagnostic codes with threshold logic.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with BMI values >= 40 (for all campaigns)
people_with_high_bmi AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_bmi_date,
        MAX(CAST(obs.result_value AS FLOAT)) AS highest_bmi_value,
        'BMI >= 40' AS evidence_type,
        cc.audit_end_date
    FROM ({{ get_observations("'BMI_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND obs.result_value IS NOT NULL
        AND TRY_CAST(obs.result_value AS FLOAT) IS NOT NULL
        AND CAST(obs.result_value AS FLOAT) >= 40
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with severe obesity diagnosis codes (for all campaigns)
people_with_severe_obesity_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_obesity_diagnosis_date,
        'Severe obesity diagnosis' AS evidence_type,
        cc.audit_end_date
    FROM ({{ get_observations("'SEV_OBESITY_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Combine all severe obesity evidence (for all campaigns)
all_obesity_evidence AS (
    SELECT 
        campaign_id,
        person_id, 
        latest_bmi_date AS evidence_date, 
        evidence_type,
        'BMI value: ' || highest_bmi_value AS evidence_detail,
        audit_end_date
    FROM people_with_high_bmi
    
    UNION ALL
    
    SELECT 
        campaign_id,
        person_id, 
        latest_obesity_diagnosis_date AS evidence_date, 
        evidence_type,
        'Severe obesity diagnosis code' AS evidence_detail,
        audit_end_date
    FROM people_with_severe_obesity_diagnosis
),

-- Step 4: Get the most recent evidence per person (for all campaigns)
best_obesity_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        evidence_type,
        evidence_date,
        evidence_detail,
        audit_end_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY evidence_date DESC) AS rn
    FROM all_obesity_evidence
),

-- Step 5: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        boe.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Morbid Obesity' AS risk_group,
        boe.person_id,
        boe.evidence_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Adults aged 18-64 with severe obesity (BMI 40+)' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        boe.audit_end_date AS created_at
    FROM best_obesity_evidence boe
    JOIN all_campaigns cc
        ON boe.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON boe.person_id = demo.person_id
    WHERE boe.rn = 1  -- Only the most recent evidence per person
        -- Apply age restrictions: 18 to under 65 years (216 months to under 65 years)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 216
)

SELECT * FROM final_eligibility
ORDER BY person_id