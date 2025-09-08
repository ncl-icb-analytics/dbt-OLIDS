/*
Under 65 At Risk - Flu Vaccination Eligibility Parent Category

This parent category aggregates all clinical condition-based eligibility
for people under 65 years old. It provides a single view of the "under 65 at risk" 
cohort whilst preserving individual condition details.

Business Rule: Person is eligible if they are:
1. Under 65 years of age at campaign reference date, AND
2. Have ANY clinical condition that makes them eligible for flu vaccination

This complements the age-based eligibility (65+, children) by providing
a clear view of the clinical risk-based cohort under 65.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Aggregate all clinical condition eligibility for under 65s
clinical_conditions_under_65 AS (
    -- Clinical conditions from the existing intermediate models
    -- Note: Standardizing to 11 columns - some models have extra debugging columns
    SELECT 
        campaign_id,
        person_id,
        campaign_category,
        risk_group,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        created_at
    FROM (
        -- All models standardized to same 11 columns
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_chronic_heart_disease') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_chronic_liver_disease') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_chronic_neurological_disease') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_asplenia') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_learning_disability') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_household_immunocompromised') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_asthma_admission') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_active_asthma_management') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_chronic_kidney_disease') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_diabetes') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_immunosuppression') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_chronic_respiratory_disease') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_health_social_care_worker') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_homeless') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_long_term_residential_care') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_pregnancy') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_severe_obesity') }}
        UNION ALL
        SELECT 
            campaign_id, person_id, campaign_category, risk_group, qualifying_event_date,
            reference_date, description, birth_date_approx, age_months_at_ref_date,
            age_years_at_ref_date, created_at
        FROM {{ ref('int_flu_carer') }}
    ) clinical_conditions
),

-- Filter to under 65 at reference date using correct age calculation
under_65_clinical_eligibility AS (
    SELECT 
        cc.campaign_id,
        cc.person_id,
        cc.campaign_category,
        cc.risk_group,
        cc.qualifying_event_date,
        cc.reference_date,
        cc.description,
        cc.birth_date_approx,
        cc.age_months_at_ref_date,
        cc.age_years_at_ref_date,
        cc.created_at
    FROM clinical_conditions_under_65 cc
    JOIN all_campaigns ac ON cc.campaign_id = ac.campaign_id
    -- Apply under 65 filter using correct age calculation
    WHERE cc.birth_date_approx > DATEADD('year', -65, ac.campaign_reference_date)
),

-- Create parent category records (one per person per campaign)
parent_category AS (
    SELECT 
        campaign_id,
        'Age-Based' AS campaign_category,
        'Under 65 At Risk' AS risk_group,
        person_id,
        MIN(qualifying_event_date) AS qualifying_event_date, -- Earliest qualifying condition
        reference_date,
        'Clinical condition(s) making person under 65 eligible for flu vaccination' AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        MAX(created_at) AS created_at -- Most recent processing time
    FROM under_65_clinical_eligibility
    GROUP BY 
        campaign_id, 
        person_id, 
        reference_date, 
        birth_date_approx, 
        age_months_at_ref_date, 
        age_years_at_ref_date
)

SELECT * FROM parent_category
ORDER BY campaign_id, person_id