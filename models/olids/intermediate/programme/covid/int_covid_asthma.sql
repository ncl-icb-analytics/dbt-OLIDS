/*
COVID Asthma Eligibility Rule (Complex)

Business Rule: Person is eligible if they have:
1. An asthma diagnosis (AST_COD) - any time in history
2. AND recent evidence of active asthma management:
   - Asthma inhaled medication (ASTRXM1_COD) since lookback date (12 months), AND
   - 2+ oral steroid prescriptions (ASTRXM2_COD) within any 2-year window, OR
3. OR asthma hospital admission (ASTADM_COD) in last 2 years (sufficient alone)
4. AND aged 5+ years (minimum age for COVID vaccination)

This implements the complex UKHSA steroid window logic with 3 overlapping 2-year periods.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with asthma diagnosis (for all campaigns)
people_with_asthma_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_asthma_date,
        cc.audit_end_date
    FROM ({{ get_observations("'AST_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with recent asthma hospital admissions (sufficient alone)
people_with_asthma_admissions AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admission_date,
        cc.audit_end_date
    FROM ({{ get_observations("'ASTADM_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.asthma_admission_lookback_date  -- 2 years before campaign
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Find people with recent asthma inhaled medications
people_with_recent_asthma_inhalers AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_inhaler_date,
        cc.audit_end_date
    FROM ({{ get_medication_orders(cluster_id='ASTRXM1_COD', source='UKHSA_COVID') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.asthma_medication_lookback_date  -- 12 months before campaign
        AND med.order_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, med.person_id, cc.audit_end_date
),

-- Step 4: Find oral steroid prescriptions in each 2-year window
-- Window 1: 2 years before autumn start
oral_steroids_window_1 AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MIN(med.order_date) AS earliest_steroid_w1,
        MAX(med.order_date) AS latest_steroid_w1,
        COUNT(*) AS steroid_count_w1
    FROM ({{ get_medication_orders(cluster_id='ASTRXM2_COD', source='UKHSA_COVID') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.asthma_steroid_window_1_start
        AND med.order_date <= cc.asthma_steroid_window_1_end
    GROUP BY cc.campaign_id, med.person_id
),

-- Window 2: 2 years from spring campaigns
oral_steroids_window_2 AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MIN(med.order_date) AS earliest_steroid_w2,
        MAX(med.order_date) AS latest_steroid_w2,
        COUNT(*) AS steroid_count_w2
    FROM ({{ get_medication_orders(cluster_id='ASTRXM2_COD', source='UKHSA_COVID') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.asthma_steroid_window_2_start
        AND med.order_date <= cc.asthma_steroid_window_2_end
    GROUP BY cc.campaign_id, med.person_id
),

-- Window 3: Additional overlapping window
oral_steroids_window_3 AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MIN(med.order_date) AS earliest_steroid_w3,
        MAX(med.order_date) AS latest_steroid_w3,
        COUNT(*) AS steroid_count_w3
    FROM ({{ get_medication_orders(cluster_id='ASTRXM2_COD', source='UKHSA_COVID') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.asthma_steroid_window_3_start
        AND med.order_date <= cc.asthma_steroid_window_3_end
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 5: Apply steroid window logic (UKHSA business rules)
people_with_qualifying_steroids AS (
    SELECT 
        COALESCE(w1.campaign_id, w2.campaign_id, w3.campaign_id) AS campaign_id,
        COALESCE(w1.person_id, w2.person_id, w3.person_id) AS person_id,
        GREATEST(
            COALESCE(w1.latest_steroid_w1, '1900-01-01'),
            COALESCE(w2.latest_steroid_w2, '1900-01-01'),
            COALESCE(w3.latest_steroid_w3, '1900-01-01')
        ) AS latest_steroid_date,
        -- UKHSA logic: 2+ prescriptions within any window OR prescriptions spanning windows within 731 days
        CASE 
            WHEN COALESCE(w1.steroid_count_w1, 0) >= 2 
                OR COALESCE(w2.steroid_count_w2, 0) >= 2 
                OR COALESCE(w3.steroid_count_w3, 0) >= 2
            THEN TRUE
            WHEN w1.earliest_steroid_w1 IS NOT NULL AND w2.earliest_steroid_w2 IS NOT NULL
                AND DATEDIFF('day', w1.latest_steroid_w1, w2.earliest_steroid_w2) <= 731
            THEN TRUE  
            WHEN w2.earliest_steroid_w2 IS NOT NULL AND w3.earliest_steroid_w3 IS NOT NULL
                AND DATEDIFF('day', w2.latest_steroid_w2, w3.earliest_steroid_w3) <= 731
            THEN TRUE
            ELSE FALSE
        END AS has_qualifying_steroids
    FROM oral_steroids_window_1 w1
    FULL OUTER JOIN oral_steroids_window_2 w2 
        ON w1.campaign_id = w2.campaign_id AND w1.person_id = w2.person_id
    FULL OUTER JOIN oral_steroids_window_3 w3
        ON COALESCE(w1.campaign_id, w2.campaign_id) = w3.campaign_id 
        AND COALESCE(w1.person_id, w2.person_id) = w3.person_id
),

-- Step 6: Combine all asthma eligibility criteria
people_with_asthma_eligibility AS (
    SELECT 
        pad.campaign_id,
        pad.person_id,
        pad.first_asthma_date,
        pwaa.latest_admission_date,
        prai.latest_inhaler_date,
        pws.latest_steroid_date,
        pws.has_qualifying_steroids,
        cc.campaign_reference_date,
        -- Eligibility logic: 
        -- 1. Admission in last 2 years (sufficient alone), OR
        -- 2. Diagnosis + recent inhaler + qualifying steroids
        CASE 
            WHEN pwaa.latest_admission_date IS NOT NULL THEN TRUE
            WHEN pad.first_asthma_date IS NOT NULL 
                AND prai.latest_inhaler_date IS NOT NULL
                AND pws.has_qualifying_steroids = TRUE THEN TRUE
            ELSE FALSE
        END AS is_eligible,
        CASE 
            WHEN pwaa.latest_admission_date IS NOT NULL THEN 'Emergency asthma admission'
            WHEN pad.first_asthma_date IS NOT NULL 
                AND prai.latest_inhaler_date IS NOT NULL
                AND pws.has_qualifying_steroids = TRUE THEN 'Active asthma with repeated steroid use'
            ELSE 'Not eligible'
        END AS eligibility_reason
    FROM people_with_asthma_diagnosis pad
    LEFT JOIN people_with_asthma_admissions pwaa 
        ON pad.campaign_id = pwaa.campaign_id AND pad.person_id = pwaa.person_id
    LEFT JOIN people_with_recent_asthma_inhalers prai
        ON pad.campaign_id = prai.campaign_id AND pad.person_id = prai.person_id  
    LEFT JOIN people_with_qualifying_steroids pws
        ON pad.campaign_id = pws.campaign_id AND pad.person_id = pws.person_id
    LEFT JOIN all_campaigns cc ON pad.campaign_id = cc.campaign_id
    WHERE pad.first_asthma_date IS NOT NULL
),

-- Step 7: Add age information and apply age restrictions
people_with_asthma_eligible_with_age AS (
    SELECT 
        pwae.campaign_id,
        pwae.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pwae.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pwae.campaign_reference_date) AS age_months_at_ref_date,
        COALESCE(pwae.latest_admission_date, pwae.latest_steroid_date, pwae.first_asthma_date) AS qualifying_event_date,
        pwae.campaign_reference_date,
        pwae.eligibility_reason
    FROM people_with_asthma_eligibility pwae
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pwae.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND pwae.is_eligible = TRUE
        AND DATEDIFF('year', demo.birth_date_approx, pwae.campaign_reference_date) >= 5  -- Minimum age 5
),

-- Step 8: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Asthma' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Asthma patient: ', eligibility_reason) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_asthma_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id