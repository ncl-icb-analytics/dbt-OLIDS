/*
Flu Pregnancy Eligibility Rule

Business Rule: Person is eligible if they have:
1. Scenario A: Pregnant at campaign start (9 months before) with no subsequent delivery, OR
2. Scenario B: Became pregnant between campaign start and reference date (remains eligible even if delivered)
3. AND aged 12 years or older (minimum age for pregnancy flu vaccination)

Campaign-specific logic:
- Scenario A: Pregnant 9 months before campaign start with no subsequent delivery
- Scenario B: Pregnancy code between campaign_start_date and campaign_reference_date

Simplified rule - focuses on flu season timing rather than complex pregnancy state logic.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Scenario B - Became pregnant during campaign period (for all campaigns)
people_pregnant_during_campaign AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_pregnancy_date,
        'Pregnant during flu campaign period' AS eligibility_reason,
        cc.audit_end_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.campaign_start_date
        AND obs.clinical_effective_date <= cc.campaign_reference_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Scenario A - Pregnant at campaign start (9 months before) with no subsequent delivery
-- First, find people pregnant at the pregnancy eligibility date (9 months before campaign start)
people_pregnant_at_campaign_start AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS pregnancy_date,
        DATEADD('month', -9, cc.campaign_start_date) AS pregnancy_eligibility_date,
        cc.audit_end_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        -- Pregnant around 9 months before campaign start (±3 month window)
        AND obs.clinical_effective_date BETWEEN 
            DATEADD('month', -12, cc.campaign_start_date) AND 
            DATEADD('month', -6, cc.campaign_start_date)
    GROUP BY cc.campaign_id, obs.person_id, cc.campaign_start_date, cc.audit_end_date
),

-- Step 3: Create lookup of pregnancy-only codes (to identify delivery/termination codes)
pregnancy_only_codes AS (
    SELECT DISTINCT mapped_concept_code
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU') }})
),

-- Step 4: Check for delivery codes after pregnancy date but before campaign start
people_with_delivery_after_pregnancy AS (
    SELECT DISTINCT
        pp.campaign_id,
        pp.person_id
    FROM people_pregnant_at_campaign_start pp
    JOIN all_campaigns cc
        ON pp.campaign_id = cc.campaign_id
    JOIN ({{ get_observations("'PREGDEL_COD'", 'UKHSA_FLU') }}) del_obs
        ON pp.person_id = del_obs.person_id
    LEFT JOIN pregnancy_only_codes poc
        ON del_obs.mapped_concept_code = poc.mapped_concept_code
    WHERE del_obs.clinical_effective_date > pp.pregnancy_date
        AND del_obs.clinical_effective_date < cc.campaign_start_date  -- Only before campaign start
        -- Delivery/termination codes are in PREGDEL_COD but NOT in PREG_COD
        AND poc.mapped_concept_code IS NULL
),

-- Step 5: Scenario A eligible people (pregnant at start, no subsequent delivery)
people_pregnant_at_start_no_delivery AS (
    SELECT 
        pp.campaign_id,
        pp.person_id,
        pp.pregnancy_date AS qualifying_event_date,
        'Pregnant at campaign start with no subsequent delivery' AS eligibility_reason,
        pp.audit_end_date
    FROM people_pregnant_at_campaign_start pp
    LEFT JOIN people_with_delivery_after_pregnancy pd
        ON pp.campaign_id = pd.campaign_id
        AND pp.person_id = pd.person_id
    WHERE pd.person_id IS NULL  -- No delivery found
),

-- Step 6: Combine all pregnancy eligibility paths (for all campaigns)
all_pregnancy_eligibility AS (
    -- Scenario A: Pregnant at campaign start with no subsequent delivery
    SELECT 
        campaign_id,
        person_id, 
        qualifying_event_date, 
        eligibility_reason,
        audit_end_date
    FROM people_pregnant_at_start_no_delivery
    
    UNION
    
    -- Scenario B: Became pregnant during campaign period
    SELECT 
        campaign_id,
        person_id, 
        latest_pregnancy_date AS qualifying_event_date, 
        eligibility_reason,
        audit_end_date
    FROM people_pregnant_during_campaign
),

-- Step 7: Remove duplicates and get best qualifying event per person (for all campaigns)
best_pregnancy_eligibility AS (
    SELECT 
        campaign_id,
        person_id,
        eligibility_reason,
        qualifying_event_date,
        audit_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_pregnancy_eligibility
),

-- Step 8: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bpe.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Pregnancy' AS risk_group,
        bpe.person_id,
        bpe.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Women pregnant at flu campaign start or during campaign period' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        bpe.audit_end_date AS created_at
    FROM best_pregnancy_eligibility bpe
    JOIN all_campaigns cc
        ON bpe.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bpe.person_id = demo.person_id
    WHERE bpe.rn = 1  -- Only the best eligibility per person
        -- Apply age restrictions: 12 to under 65 years (144 months to under 65 years)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 144
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id