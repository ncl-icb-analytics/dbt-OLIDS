/*
Simplified Chronic Kidney Disease (CKD) Eligibility Rule

Business Rule: Person is eligible if they have:
1. A direct CKD diagnosis (CKD_COD) - earliest occurrence, OR
2. Latest CKD stage 3-5 code (CKD35_COD) is more recent than or equal to 
   latest any-stage CKD code (CKD15_COD)
3. AND aged 6 months or older (minimum age for flu vaccination)

The hierarchical logic ensures people with more recent severe CKD stages are included,
even if they have older general CKD codes. This replaces the complex macro logic
with clear, step-by-step SQL.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with direct CKD diagnosis (for all campaigns)
people_with_ckd_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_ckd_date,
        'Direct CKD diagnosis' AS eligibility_reason,
        cc.audit_end_date
    FROM ({{ get_observations("'CKD_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with any-stage CKD codes (latest occurrence, for all campaigns)
people_with_any_stage_ckd AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_any_stage_date,
        cc.audit_end_date
    FROM ({{ get_observations("'CKD15_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Find people with stage 3-5 CKD codes (latest occurrence, for all campaigns)
people_with_severe_ckd AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_severe_stage_date,
        cc.audit_end_date
    FROM ({{ get_observations("'CKD35_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 4: Apply hierarchical logic for stage-based eligibility (for all campaigns)
people_eligible_via_ckd_stages AS (
    SELECT 
        severe.campaign_id,
        severe.person_id,
        severe.latest_severe_stage_date AS qualifying_date,
        'CKD stage 3-5 (more recent than any-stage code)' AS eligibility_reason,
        'Latest severe stage: ' || severe.latest_severe_stage_date || 
        ', Latest any stage: ' || COALESCE(any_stage.latest_any_stage_date::VARCHAR, 'none') AS stage_comparison,
        severe.audit_end_date
    FROM people_with_severe_ckd severe
    LEFT JOIN people_with_any_stage_ckd any_stage
        ON severe.campaign_id = any_stage.campaign_id
        AND severe.person_id = any_stage.person_id
    WHERE severe.latest_severe_stage_date >= COALESCE(any_stage.latest_any_stage_date, severe.latest_severe_stage_date)
),

-- Step 5: Combine all CKD eligibility paths (for all campaigns)
all_ckd_eligible_people AS (
    -- Path 1: Direct CKD diagnosis
    SELECT 
        campaign_id,
        person_id,
        first_ckd_date AS qualifying_event_date,
        eligibility_reason,
        NULL AS stage_comparison,
        audit_end_date
    FROM people_with_ckd_diagnosis
    
    UNION
    
    -- Path 2: Stage-based hierarchy
    SELECT 
        campaign_id,
        person_id,
        qualifying_date AS qualifying_event_date,
        eligibility_reason,
        stage_comparison,
        audit_end_date
    FROM people_eligible_via_ckd_stages
),

-- Step 6: Remove duplicates and pick best qualifying event per person (for all campaigns)
best_ckd_eligibility AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date,
        eligibility_reason,
        stage_comparison,
        audit_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_ckd_eligible_people
),

-- Step 7: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bce.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Chronic Kidney Disease (Stage 3-5)' AS risk_group,
        bce.person_id,
        bce.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with chronic kidney disease stage 3-5' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        bce.audit_end_date AS created_at
    FROM best_ckd_eligibility bce
    JOIN all_campaigns cc
        ON bce.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bce.person_id = demo.person_id
    WHERE bce.rn = 1  -- Only the best eligibility per person
        -- Apply minimum age restriction: 6 months (minimum age for flu vaccination)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY person_id