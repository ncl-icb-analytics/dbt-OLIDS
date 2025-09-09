/*
COVID Gestational Diabetes Eligibility Rule

Business Rule: Person is eligible if they have:
1. Gestational diabetes diagnosis (GDM_COD) during pregnancy
2. AND currently pregnant (pregnancy logic)
3. AND aged 16+ years (pregnancy eligibility age)
4. Only eligible in 2024/25 campaigns (broader eligibility)

Pregnancy-specific diabetes that occurs during pregnancy.
This condition is NOT eligible in 2025/26 restricted campaigns.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with gestational diabetes diagnosis (for all campaigns)
people_with_gdm_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date AS gdm_date,
        cc.audit_end_date,
        cc.campaign_reference_date
    FROM ({{ get_observations("'GDIAB_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
        -- Only include if this condition is eligible in the campaign
        AND cc.eligible_gestational_diabetes = TRUE
),

-- Step 2: Get pregnancy information (reuse COVID pregnancy logic)
pregnancy_eligible AS (
    SELECT 
        campaign_id,
        person_id,
        qualifying_event_date AS pregnancy_start_date,
        reference_date,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date
    FROM {{ ref('int_covid_pregnancy') }}
),

-- Step 3: Match gestational diabetes with current pregnancy
people_with_gdm_and_pregnancy AS (
    SELECT 
        pgdm.campaign_id,
        pgdm.person_id,
        pgdm.gdm_date,
        pe.pregnancy_start_date,
        pe.birth_date_approx,
        pe.age_months_at_ref_date,
        pe.age_years_at_ref_date,
        pe.reference_date AS campaign_reference_date,
        -- GDM must be within pregnancy period or close to it
        CASE 
            WHEN pgdm.gdm_date >= pe.pregnancy_start_date 
                OR DATEDIFF('day', pgdm.gdm_date, pe.pregnancy_start_date) <= 90  -- GDM within 3 months before pregnancy
            THEN TRUE 
            ELSE FALSE 
        END AS is_gdm_during_pregnancy
    FROM people_with_gdm_diagnosis pgdm
    INNER JOIN pregnancy_eligible pe 
        ON pgdm.campaign_id = pe.campaign_id AND pgdm.person_id = pe.person_id
),

-- Step 4: Format for eligibility table
final_eligible AS (
    SELECT 
        campaign_id,
        'CLINICAL_CONDITION' AS campaign_category,
        'Gestational Diabetes' AS risk_group,
        person_id,
        gdm_date AS qualifying_event_date,
        campaign_reference_date AS reference_date,
        'Gestational diabetes during pregnancy' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_with_gdm_and_pregnancy
    WHERE is_gdm_during_pregnancy = TRUE
)

SELECT * FROM final_eligible
ORDER BY campaign_id, person_id