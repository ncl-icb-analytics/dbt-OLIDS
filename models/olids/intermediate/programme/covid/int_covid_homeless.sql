/*
COVID Homelessness Eligibility Rule

Business Rule: Person is eligible if they have:
1. Homeless status or accommodation code (HOMELESS_COD) - recent status
2. AND aged 5+ years (minimum age for COVID vaccination)
3. Only eligible in 2024/25 campaigns (broader eligibility)

Recent homelessness status - within 2 years of campaign reference date.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with homeless status (for all campaigns)
people_with_homeless_status AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_homeless_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'HOMELESS_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_homeless = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date,
        cc.campaign_reference_date
),

-- Step 2: Find people with residence codes (for comparison)
people_with_residence_status AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_residence_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        AND cc.eligible_homeless = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 3: Apply homeless business logic (homeless date must be >= residence date)
people_with_valid_homeless_status AS (
    SELECT 
        phs.campaign_id,
        phs.person_id,
        phs.latest_homeless_date,
        prs.latest_residence_date,
        phs.campaign_reference_date,
        -- Homeless logic: IF HOMELESS_DAT ≥ RESIDE_DAT: Select
        CASE 
            WHEN prs.latest_residence_date IS NULL THEN TRUE  -- No residence code, homeless valid
            WHEN phs.latest_homeless_date >= prs.latest_residence_date THEN TRUE
            ELSE FALSE
        END AS is_currently_homeless
    FROM people_with_homeless_status phs
    LEFT JOIN people_with_residence_status prs
        ON phs.campaign_id = prs.campaign_id AND phs.person_id = prs.person_id
    WHERE phs.latest_homeless_date IS NOT NULL
),

-- Step 4: Add age information and apply age restrictions  
people_with_homeless_eligible_with_age AS (
    SELECT 
        pvhs.campaign_id,
        pvhs.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pvhs.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pvhs.campaign_reference_date) AS age_months_at_ref_date,
        pvhs.latest_homeless_date AS qualifying_event_date,
        pvhs.campaign_reference_date
    FROM people_with_valid_homeless_status pvhs
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pvhs.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND pvhs.is_currently_homeless = TRUE
        AND DATEDIFF('year', demo.birth_date_approx, pvhs.campaign_reference_date) >= 5  -- Minimum age 5
),

-- Step 5: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Homelessness' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Latest residence status is homeless' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_homeless_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id