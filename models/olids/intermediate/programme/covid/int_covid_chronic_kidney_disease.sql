/*
COVID Chronic Kidney Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. Chronic kidney disease diagnosis (CKD_COV_COD) - any time, OR
2. CKD stage codes (CKD15_COD) where latest stage is 3-5 (CKD35_COD)
3. AND aged 5+ years (minimum age for COVID vaccination)
4. Only eligible in 2024/25 campaigns (broader eligibility)

Hierarchical rule - stage codes override diagnosis if present.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with CKD diagnosis (for all campaigns)
people_with_ckd_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_ckd_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'CKD_COV_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_chronic_kidney_disease = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date,
        cc.campaign_reference_date
),

-- Step 2: Find people with any CKD stage codes (for all campaigns)
people_with_ckd_stages AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS stage_date,
        obs.mapped_concept_code AS stage_code,
        cc.audit_end_date,
        cc.campaign_reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY cc.campaign_id, obs.person_id 
            ORDER BY obs.clinical_effective_date DESC, obs.mapped_concept_code DESC
        ) AS stage_rank
    FROM ({{ get_observations("'CKD15_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_chronic_kidney_disease = TRUE
),

-- Step 3: Find people with stage 3-5 CKD codes (for all campaigns)
people_with_ckd_stages_3_5 AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS stage_3_5_date,
        obs.mapped_concept_code AS stage_3_5_code,
        cc.audit_end_date,
        cc.campaign_reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY cc.campaign_id, obs.person_id 
            ORDER BY obs.clinical_effective_date DESC, obs.mapped_concept_code DESC
        ) AS stage_3_5_rank
    FROM ({{ get_observations("'CKD35_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_chronic_kidney_disease = TRUE
),

-- Step 4: Apply CKD business logic
people_with_ckd_eligible AS (
    SELECT 
        COALESCE(pcd.campaign_id, pcs.campaign_id) AS campaign_id,
        COALESCE(pcd.person_id, pcs.person_id) AS person_id,
        pcd.first_ckd_date,
        pcs.stage_date AS latest_stage_date,
        pcs.stage_code AS latest_stage_code,
        pcs35.stage_3_5_date AS latest_stage_3_5_date,
        COALESCE(pcd.campaign_reference_date, pcs.campaign_reference_date) AS campaign_reference_date,
        -- CKD eligibility logic: diagnosis OR (stage codes present AND latest is 3-5)
        CASE 
            WHEN pcd.first_ckd_date IS NOT NULL THEN TRUE
            WHEN pcs.stage_date IS NOT NULL AND pcs35.stage_3_5_date = pcs.stage_date THEN TRUE
            ELSE FALSE
        END AS is_ckd_eligible,
        CASE 
            WHEN pcd.first_ckd_date IS NOT NULL THEN 'CKD diagnosis'
            WHEN pcs.stage_date IS NOT NULL AND pcs35.stage_3_5_date = pcs.stage_date THEN 'CKD stage 3-5'
            ELSE 'Not eligible'
        END AS eligibility_reason
    FROM people_with_ckd_diagnosis pcd
    FULL OUTER JOIN people_with_ckd_stages pcs 
        ON pcd.campaign_id = pcs.campaign_id AND pcd.person_id = pcs.person_id AND pcs.stage_rank = 1
    LEFT JOIN people_with_ckd_stages_3_5 pcs35
        ON COALESCE(pcd.campaign_id, pcs.campaign_id) = pcs35.campaign_id 
        AND COALESCE(pcd.person_id, pcs.person_id) = pcs35.person_id
        AND pcs35.stage_3_5_rank = 1
    WHERE (pcd.person_id IS NOT NULL OR pcs.person_id IS NOT NULL)
),

-- Step 5: Add age information and apply age restrictions
people_with_ckd_eligible_with_age AS (
    SELECT 
        pce.campaign_id,
        pce.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pce.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pce.campaign_reference_date) AS age_months_at_ref_date,
        COALESCE(pce.latest_stage_3_5_date, pce.latest_stage_date, pce.first_ckd_date) AS qualifying_event_date,
        pce.campaign_reference_date,
        pce.eligibility_reason
    FROM people_with_ckd_eligible pce
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pce.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND pce.is_ckd_eligible = TRUE
        AND DATEDIFF('year', demo.birth_date_approx, pce.campaign_reference_date) >= 5  -- Minimum age 5
),

-- Step 6: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Chronic Kidney Disease' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Chronic kidney disease: ', eligibility_reason) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_ckd_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id