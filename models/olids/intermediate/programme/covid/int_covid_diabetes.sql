/*
COVID Diabetes Eligibility Rule

Business Rule: Person is eligible if they have:
1. Diabetes diagnosis (DIAB_COD) - latest occurrence
2. AND NOT resolved diabetes (DMRES_COD more recent than diagnosis), OR
3. Addison's disease/pan-hypopituitarism (ADDIS_COD) - any time, OR  
4. Gestational diabetes (via separate int_covid_gestational_diabetes model)
5. AND aged 5+ years (minimum age for COVID vaccination)
6. Only eligible in 2024/25 campaigns (broader eligibility)

Exclusion rule - resolved diabetes patients are excluded unless more recent diagnosis.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with diabetes diagnosis (for all campaigns)
people_with_diabetes_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_diabetes_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'DIAB_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_diabetes = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date,
        cc.campaign_reference_date
),

-- Step 2: Find people with resolved diabetes codes (for all campaigns)
people_with_diabetes_resolved AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_resolved_date,
        cc.audit_end_date
    FROM ({{ get_observations("'DMRES_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_diabetes = TRUE
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Find people with Addison's disease (for all campaigns)
people_with_addisons AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_addisons_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'ADDIS_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_diabetes = TRUE
    GROUP BY 
        cc.campaign_id, obs.person_id, cc.audit_end_date, cc.campaign_reference_date
),

-- Step 4: Apply diabetes business logic (exclusion rule)
people_with_diabetes_eligible AS (
    SELECT 
        COALESCE(pdd.campaign_id, pwa.campaign_id) AS campaign_id,
        COALESCE(pdd.person_id, pwa.person_id) AS person_id,
        pdd.latest_diabetes_date,
        pdr.latest_resolved_date,
        pwa.latest_addisons_date,
        COALESCE(pdd.campaign_reference_date, pwa.campaign_reference_date) AS campaign_reference_date,
        -- Diabetes eligibility logic: 
        -- 1. Addison's (always eligible), OR
        -- 2. Diabetes diagnosis more recent than resolved (or no resolved code)
        CASE 
            WHEN pwa.latest_addisons_date IS NOT NULL THEN TRUE
            WHEN pdd.latest_diabetes_date IS NOT NULL 
                AND (pdr.latest_resolved_date IS NULL OR pdd.latest_diabetes_date > pdr.latest_resolved_date) THEN TRUE
            ELSE FALSE
        END AS is_diabetes_eligible,
        CASE 
            WHEN pwa.latest_addisons_date IS NOT NULL THEN 'Addisons disease/pan-hypopituitarism'
            WHEN pdd.latest_diabetes_date IS NOT NULL 
                AND (pdr.latest_resolved_date IS NULL OR pdd.latest_diabetes_date > pdr.latest_resolved_date) THEN 'Active diabetes'
            ELSE 'Not eligible'
        END AS eligibility_reason
    FROM people_with_diabetes_diagnosis pdd
    FULL OUTER JOIN people_with_addisons pwa
        ON pdd.campaign_id = pwa.campaign_id AND pdd.person_id = pwa.person_id
    LEFT JOIN people_with_diabetes_resolved pdr
        ON COALESCE(pdd.campaign_id, pwa.campaign_id) = pdr.campaign_id 
        AND COALESCE(pdd.person_id, pwa.person_id) = pdr.person_id
    WHERE (pdd.person_id IS NOT NULL OR pwa.person_id IS NOT NULL)
),

-- Step 5: Add age information and apply age restrictions
people_with_diabetes_eligible_with_age AS (
    SELECT 
        pde.campaign_id,
        pde.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pde.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pde.campaign_reference_date) AS age_months_at_ref_date,
        COALESCE(pde.latest_addisons_date, pde.latest_diabetes_date) AS qualifying_event_date,
        pde.campaign_reference_date,
        pde.eligibility_reason
    FROM people_with_diabetes_eligible pde
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pde.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
        AND pde.is_diabetes_eligible = TRUE
        AND DATEDIFF('year', demo.birth_date_approx, pde.campaign_reference_date) >= 5  -- Minimum age 5
),

-- Step 6: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Diabetes' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('Diabetes: ', eligibility_reason) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_diabetes_eligible_with_age
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id