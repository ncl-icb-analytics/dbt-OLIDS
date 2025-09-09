/*
COVID Vaccination Eligibility Fact Table

This model determines who is ELIGIBLE for COVID vaccination using clear, 
individual rule models instead of complex macros.

Key improvements:
- Each rule is implemented in its own clear model
- Business logic is explicit and documented
- Terminology is descriptive  
- Single configuration point for dates
- Direct use of core macros (get_observations, get_medication_orders)
- Works with multiple campaigns via covid_campaign_config macro
- Separate from vaccination status tracking (see fct_covid_status)

Multi-Campaign Support:
- covid_2024_autumn: September 2024 - March 2025 (broader eligibility)
- covid_2025_spring: April 2025 - June 2025 (broader eligibility)  
- covid_2025_autumn: September 2025 - March 2026 (restricted eligibility)

Usage: 
- Default: Uses all defined COVID campaigns automatically
- Specific campaign analysis: Filter by campaign_id in downstream models
- For vaccination tracking, use fct_covid_status instead
- This replaces all the old complex macro-based models
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'person_id', 'campaign_category']
) }}

WITH
-- Age-based eligibility (all campaigns automatically included from intermediate models)
age_based_eligibility AS (
    -- Age 75 Plus (universal eligibility)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_age_75_plus') }}
),

-- Simple clinical condition eligibility
clinical_condition_eligibility AS (
    -- Chronic Heart Disease (campaign-specific eligibility)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_chronic_heart_disease') }}
    
    UNION ALL
    
    -- Chronic Liver Disease
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_chronic_liver_disease') }}
    
    UNION ALL
    
    -- Chronic Neurological Disease
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_chronic_neurological_disease') }}
    
    UNION ALL
    
    -- Asplenia
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_asplenia') }}
    
    UNION ALL
    
    -- Learning Disability
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_learning_disability') }}
    
    UNION ALL
    
    -- Severe Mental Illness
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_severe_mental_illness') }}
),

-- Complex clinical condition eligibility (combination/hierarchical/exclusion rules)
complex_clinical_eligibility AS (
    -- Asthma (complex steroid window logic)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_asthma') }}
    
    UNION ALL
    
    -- Chronic Kidney Disease (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_chronic_kidney_disease') }}
    
    UNION ALL
    
    -- Diabetes (exclusion rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'EXCLUSION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_diabetes') }}
    
    UNION ALL
    
    -- Immunosuppression (combination rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_immunosuppression') }}
    
    UNION ALL
    
    -- Chronic Respiratory Disease (combination rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_chronic_respiratory_disease') }}
    
    UNION ALL
    
    -- Morbid Obesity (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_morbid_obesity') }}
    
    UNION ALL
    
    -- Homeless (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_homeless') }}
    
    UNION ALL
    
    -- Long-term Residential Care (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_long_term_residential_care') }}
    
    UNION ALL
    
    -- Pregnancy (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_pregnancy') }}
    
    UNION ALL
    
    -- Gestational Diabetes (exclusion rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'EXCLUSION' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_covid_gestational_diabetes') }}
),

-- Union all eligibility types (vaccination tracking removed - belongs in separate table)
all_eligibility AS (
    SELECT * FROM age_based_eligibility
    UNION ALL
    SELECT * FROM clinical_condition_eligibility  
    UNION ALL
    SELECT * FROM complex_clinical_eligibility
),

-- Final formatting (campaign information already included in intermediate models)
final_eligibility AS (
    SELECT 
        campaign_id,
        campaign_category,
        risk_group,
        person_id,
        qualifying_event_date,
        reference_date,
        description AS eligibility_reason,
        rule_type,
        eligibility_priority,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        created_at
    FROM all_eligibility
)

SELECT * FROM final_eligibility
ORDER BY person_id, eligibility_priority, campaign_category