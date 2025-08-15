/*
Simplified Pregnancy Eligibility Rule

Business Rule: Person is eligible if they have:
1. Group 1: Pregnancy code (PREG_COD) since campaign start date, OR
2. Group 2: Latest pregnancy/delivery event before start date is pregnancy 
   - Latest of PREG_COD or PREGDEL_COD is PREG_COD
3. AND aged 12 to under 65 years (144 months to under 65 years)

Hierarchical rule - complex pregnancy and delivery logic with date comparisons.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with pregnancy codes since campaign start (for all campaigns)
people_with_recent_pregnancy AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_pregnancy_date,
        'Pregnancy since campaign start' AS eligibility_reason,
        cc.audit_end_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.campaign_start_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with pregnancy codes before campaign start (for all campaigns)
people_with_historical_pregnancy AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_historical_pregnancy_date,
        cc.audit_end_date
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date < cc.campaign_start_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Find people with delivery codes before campaign start (for all campaigns)
people_with_historical_delivery AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_historical_delivery_date,
        cc.audit_end_date
    FROM ({{ get_observations("'PREGDEL_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date < cc.campaign_start_date
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 4: Apply Group 2 logic - latest pregnancy/delivery event is pregnancy (for all campaigns)
people_with_pregnancy_before_delivery AS (
    SELECT 
        hp.campaign_id,
        hp.person_id,
        hp.latest_historical_pregnancy_date,
        'Latest pregnancy/delivery before start date is pregnancy' AS eligibility_reason,
        'Pregnancy: ' || hp.latest_historical_pregnancy_date || 
        ', Delivery: ' || COALESCE(hd.latest_historical_delivery_date::VARCHAR, 'none') AS comparison_detail,
        hp.audit_end_date
    FROM people_with_historical_pregnancy hp
    LEFT JOIN people_with_historical_delivery hd
        ON hp.campaign_id = hd.campaign_id
        AND hp.person_id = hd.person_id
    WHERE hp.latest_historical_pregnancy_date >= COALESCE(hd.latest_historical_delivery_date, hp.latest_historical_pregnancy_date)
),

-- Step 5: Combine all pregnancy eligibility paths (for all campaigns)
all_pregnancy_eligibility AS (
    -- Group 1: Recent pregnancy since campaign start
    SELECT 
        campaign_id,
        person_id, 
        latest_pregnancy_date AS qualifying_event_date, 
        eligibility_reason,
        audit_end_date
    FROM people_with_recent_pregnancy
    
    UNION
    
    -- Group 2: Latest pregnancy/delivery before start is pregnancy
    SELECT 
        campaign_id,
        person_id, 
        latest_historical_pregnancy_date AS qualifying_event_date, 
        eligibility_reason,
        audit_end_date
    FROM people_with_pregnancy_before_delivery
),

-- Step 6: Remove duplicates and get best qualifying event per person (for all campaigns)
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

-- Step 7: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        bpe.campaign_id,
        'PREG_GROUP' AS rule_group_id,
        'Pregnant' AS rule_group_name,
        bpe.person_id,
        bpe.qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Pregnant women aged 12-64' AS description,
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
        AND DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) < 65
)

SELECT * FROM final_eligibility
ORDER BY person_id