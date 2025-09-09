/*
COVID Pregnancy Eligibility Rule

Business Rule: Person is eligible if they have:
1. Pregnancy or delivery code between campaign start and end (1/9/25 to 30/6/26), OR  
2. Pregnancy code in 8 months before campaign with no subsequent delivery

Simplified rule aligned with flu pregnancy logic.
Eligible in all COVID campaigns (both 2024/25 and 2025/26).
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find pregnancy/delivery codes during campaign periods (for all campaigns)  
pregnancy_during_campaign_periods AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_pregnancy_date,
        'Pregnant/delivered during COVID campaign periods' AS eligibility_reason,
        cc.campaign_reference_date
    FROM ({{ get_observations("'PREGDEL_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.pregnancy_current_start
        AND obs.clinical_effective_date <= cc.pregnancy_current_end
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.campaign_reference_date
),

-- Step 2: Find pregnancy codes in lookback period (8 months before)
pregnancy_before_campaign AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS pregnancy_date,
        cc.pregnancy_lookback_start,
        cc.pregnancy_current_start,
        cc.campaign_reference_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.pregnancy_lookback_start
        AND obs.clinical_effective_date < cc.pregnancy_current_start
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.pregnancy_lookback_start,
        cc.pregnancy_current_start, cc.campaign_reference_date
),

-- Step 3: Create lookup of pregnancy-only codes (to identify delivery codes)
pregnancy_only_codes AS (
    SELECT DISTINCT mapped_concept_code
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_COVID') }})
),

-- Step 4: Check for delivery codes after pregnancy but before campaign start
pregnancy_with_delivery_before_campaign AS (
    SELECT DISTINCT
        pb.campaign_id,
        pb.person_id
    FROM pregnancy_before_campaign pb
    JOIN ({{ get_observations("'PREGDEL_COD'", 'UKHSA_COVID') }}) del_obs
        ON pb.person_id = del_obs.person_id
    LEFT JOIN pregnancy_only_codes poc
        ON del_obs.mapped_concept_code = poc.mapped_concept_code
    WHERE del_obs.clinical_effective_date > pb.pregnancy_date
        AND del_obs.clinical_effective_date < pb.pregnancy_current_start
        -- Delivery codes are in PREGDEL_COD but NOT in PREG_COD
        AND poc.mapped_concept_code IS NULL
),

-- Step 5: Pregnancy before campaign with no subsequent delivery  
pregnancy_before_campaign_no_delivery AS (
    SELECT 
        pb.campaign_id,
        pb.person_id,
        pb.pregnancy_date AS qualifying_event_date,
        'Pregnant before campaign with no subsequent delivery' AS eligibility_reason,
        pb.campaign_reference_date
    FROM pregnancy_before_campaign pb
    LEFT JOIN pregnancy_with_delivery_before_campaign pd
        ON pb.campaign_id = pd.campaign_id AND pb.person_id = pd.person_id
    WHERE pd.person_id IS NULL  -- No delivery found
),

-- Step 6: Combine all pregnancy eligibility paths
all_pregnancy_eligibility AS (
    -- Pregnancy/delivery during campaign periods
    SELECT 
        campaign_id, person_id, latest_pregnancy_date AS qualifying_event_date,
        eligibility_reason, campaign_reference_date
    FROM pregnancy_during_campaign_periods
    
    UNION
    
    -- Pregnancy before campaign with no delivery
    SELECT 
        campaign_id, person_id, qualifying_event_date,
        eligibility_reason, campaign_reference_date
    FROM pregnancy_before_campaign_no_delivery
),

-- Step 7: Remove duplicates and get best qualifying event per person
best_pregnancy_eligibility AS (
    SELECT 
        campaign_id, person_id, eligibility_reason, qualifying_event_date, campaign_reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY qualifying_event_date DESC, eligibility_reason
        ) AS rn
    FROM all_pregnancy_eligibility
),

-- Step 8: Add age information and apply age restrictions
people_pregnant_with_age AS (
    SELECT 
        bpe.campaign_id,
        bpe.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, bpe.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, bpe.campaign_reference_date) AS age_months_at_ref_date,
        bpe.qualifying_event_date,
        bpe.campaign_reference_date,
        bpe.eligibility_reason
    FROM best_pregnancy_eligibility bpe
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON bpe.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND bpe.rn = 1  -- Only best eligibility per person
        AND DATEDIFF('year', demo.birth_date_approx, bpe.campaign_reference_date) >= 12  -- Minimum age 12 (as per flu)
),

-- Step 9: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'SPECIAL_POPULATION' AS campaign_category,
        'Pregnancy' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Pregnant woman: ', eligibility_reason) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_pregnant_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id