/*
COVID Long-term Residential Care Eligibility Rule

Business Rule: Person is eligible if they are:
1. Currently residing in long-term residential care (care home)
2. Latest residence code is from LONGRES_COD cluster
3. AND aged 65+ at campaign reference date (for care home specific eligibility)

Hierarchical rule - latest residence status determines eligibility.
This is a key eligibility group for COVID campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with any residence codes (for all campaigns)
people_with_residence_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS residence_date,
        obs.mapped_concept_code AS residence_code,
        cc.audit_end_date
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
),

-- Step 2: Find people with long-term care residence codes (for all campaigns)
people_with_longterm_care_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS longterm_care_date,
        obs.mapped_concept_code AS longterm_care_code,
        cc.audit_end_date
    FROM ({{ get_observations("'LONGRES_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
),

-- Step 3: Get latest residence code for each person/campaign
latest_residence_status AS (
    SELECT 
        pwrc.campaign_id,
        pwrc.person_id,
        pwrc.residence_code AS latest_residence_code,
        pwrc.residence_date AS latest_residence_date,
        ROW_NUMBER() OVER (
            PARTITION BY pwrc.campaign_id, pwrc.person_id 
            ORDER BY pwrc.residence_date DESC, pwrc.residence_code DESC
        ) AS residence_rank
    FROM people_with_residence_codes pwrc
),

-- Step 4: Get latest long-term care code for each person/campaign  
latest_longterm_care_status AS (
    SELECT 
        pltc.campaign_id,
        pltc.person_id,
        pltc.longterm_care_code AS latest_longterm_care_code,
        pltc.longterm_care_date AS latest_longterm_care_date,
        ROW_NUMBER() OVER (
            PARTITION BY pltc.campaign_id, pltc.person_id 
            ORDER BY pltc.longterm_care_date DESC, pltc.longterm_care_code DESC
        ) AS longterm_care_rank
    FROM people_with_longterm_care_codes pltc
),

-- Step 5: Determine if latest residence status is long-term care
people_in_longterm_care AS (
    SELECT 
        lrs.campaign_id,
        lrs.person_id,
        lrs.latest_residence_code,
        lrs.latest_residence_date,
        ltcs.latest_longterm_care_date,
        cc.campaign_reference_date,
        cc.audit_end_date,
        -- Person is in long-term care if their latest residence code is a long-term care code
        CASE 
            WHEN ltcs.latest_longterm_care_date = lrs.latest_residence_date 
                AND ltcs.latest_longterm_care_date IS NOT NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS is_in_longterm_care
    FROM latest_residence_status lrs
    LEFT JOIN latest_longterm_care_status ltcs 
        ON lrs.campaign_id = ltcs.campaign_id 
        AND lrs.person_id = ltcs.person_id
        AND ltcs.longterm_care_rank = 1
    LEFT JOIN all_campaigns cc ON lrs.campaign_id = cc.campaign_id
    WHERE lrs.residence_rank = 1
        AND lrs.latest_residence_date IS NOT NULL
),

-- Step 6: Add age information and apply age restrictions
people_in_longterm_care_with_age AS (
    SELECT 
        pltc.campaign_id,
        pltc.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pltc.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pltc.campaign_reference_date) AS age_months_at_ref_date,
        pltc.latest_residence_date AS qualifying_event_date,
        pltc.campaign_reference_date,
        pltc.is_in_longterm_care
    FROM people_in_longterm_care pltc
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pltc.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND pltc.is_in_longterm_care = TRUE
        AND DATEDIFF('year', demo.birth_date_approx, pltc.campaign_reference_date) >= 65  -- Care home eligibility is 65+
),

-- Step 7: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'SPECIAL_POPULATION' AS campaign_category,
        'Long-term Residential Care' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Resident in long-term residential care home (aged 65+)' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_in_longterm_care_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id