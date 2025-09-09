/*
COVID Morbid Obesity Eligibility Rule

Business Rule: Person is eligible if they have:
1. BMI ≥ 40 (latest BMI value), OR
2. Severe obesity stage code (most recent stage code)
3. AND aged 18+ years (adults only - obesity not assessed in under 18s)
4. Only eligible in 2024/25 campaigns (broader eligibility)

Hierarchical rule - stage codes override BMI values if more recent.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with BMI values (for all campaigns)
people_with_bmi_values AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS bmi_date,
        CAST(obs.result_value AS FLOAT) AS bmi_value,
        cc.audit_end_date,
        cc.campaign_reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY cc.campaign_id, obs.person_id 
            ORDER BY obs.clinical_effective_date DESC
        ) AS bmi_rank
    FROM ({{ get_observations("'BMI_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND obs.result_value IS NOT NULL
        AND TRY_CAST(obs.result_value AS FLOAT) IS NOT NULL
        AND CAST(obs.result_value AS FLOAT) > 0  -- Valid BMI values only
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_morbid_obesity = TRUE
),

-- Step 2: Find people with BMI stage codes (for all campaigns)
people_with_bmi_stages AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS stage_date,
        obs.mapped_concept_code AS stage_code,
        cc.audit_end_date,
        cc.campaign_reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY cc.campaign_id, obs.person_id 
            ORDER BY obs.clinical_effective_date DESC
        ) AS stage_rank
    FROM ({{ get_observations("'BMI_STAGE_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_morbid_obesity = TRUE
),

-- Step 3: Find people with severe obesity stage codes (for all campaigns)
people_with_severe_obesity_stages AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS severe_obesity_date,
        obs.mapped_concept_code AS severe_obesity_code,
        cc.audit_end_date,
        cc.campaign_reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY cc.campaign_id, obs.person_id 
            ORDER BY obs.clinical_effective_date DESC
        ) AS severe_obesity_rank
    FROM ({{ get_observations("'SEV_OBESITY_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_morbid_obesity = TRUE
),

-- Step 4: Apply morbid obesity business logic (UKHSA rules)
people_with_morbid_obesity AS (
    SELECT 
        COALESCE(pbv.campaign_id, pbs.campaign_id, pso.campaign_id) AS campaign_id,
        COALESCE(pbv.person_id, pbs.person_id, pso.person_id) AS person_id,
        pbv.bmi_date,
        pbv.bmi_value,
        pbs.stage_date,
        pso.severe_obesity_date,
        COALESCE(pbv.campaign_reference_date, pbs.campaign_reference_date, pso.campaign_reference_date) AS campaign_reference_date,
        -- Obesity eligibility logic (UKHSA business rules):
        -- 1. Severe obesity stage code more recent than BMI value, OR
        -- 2. Severe obesity stage code exists and no BMI value, OR  
        -- 3. BMI value ≥ 40 and (no stage code OR BMI more recent than stage)
        CASE 
            WHEN pso.severe_obesity_date IS NOT NULL 
                AND (pbv.bmi_date IS NULL OR pso.severe_obesity_date >= pbv.bmi_date) THEN TRUE
            WHEN pbv.bmi_value >= 40 
                AND (pbs.stage_date IS NULL OR pbv.bmi_date >= pbs.stage_date) THEN TRUE
            ELSE FALSE
        END AS is_morbidly_obese,
        CASE 
            WHEN pso.severe_obesity_date IS NOT NULL 
                AND (pbv.bmi_date IS NULL OR pso.severe_obesity_date >= pbv.bmi_date) THEN 'Severe obesity stage code'
            WHEN pbv.bmi_value >= 40 
                AND (pbs.stage_date IS NULL OR pbv.bmi_date >= pbs.stage_date) THEN CONCAT('BMI ≥40 (', ROUND(pbv.bmi_value, 1), ')')
            ELSE 'Not morbidly obese'
        END AS obesity_reason
    FROM people_with_bmi_values pbv
    FULL OUTER JOIN people_with_bmi_stages pbs
        ON pbv.campaign_id = pbs.campaign_id AND pbv.person_id = pbs.person_id AND pbs.stage_rank = 1
    FULL OUTER JOIN people_with_severe_obesity_stages pso
        ON COALESCE(pbv.campaign_id, pbs.campaign_id) = pso.campaign_id 
        AND COALESCE(pbv.person_id, pbs.person_id) = pso.person_id
        AND pso.severe_obesity_rank = 1
    WHERE pbv.bmi_rank = 1 OR pbv.person_id IS NULL  -- Latest BMI only
        AND (pbv.person_id IS NOT NULL OR pbs.person_id IS NOT NULL OR pso.person_id IS NOT NULL)
),

-- Step 5: Add age information and apply age restrictions (adults 18+ only)
people_with_morbid_obesity_with_age AS (
    SELECT 
        pmo.campaign_id,
        pmo.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pmo.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pmo.campaign_reference_date) AS age_months_at_ref_date,
        COALESCE(pmo.severe_obesity_date, pmo.bmi_date) AS qualifying_event_date,
        pmo.campaign_reference_date,
        pmo.obesity_reason
    FROM people_with_morbid_obesity pmo
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pmo.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND pmo.is_morbidly_obese = TRUE
        AND DATEDIFF('year', demo.birth_date_approx, pmo.campaign_reference_date) >= 18  -- Adults 18+ only
),

-- Step 6: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Morbid Obesity' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Morbid obesity: ', obesity_reason) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_morbid_obesity_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id