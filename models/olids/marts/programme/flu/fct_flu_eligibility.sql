/*
Flu Vaccination Eligibility Fact Table

This model determines who is ELIGIBLE for flu vaccination using clear, 
individual rule models instead of complex macros.

Key improvements:
- Each rule is implemented in its own clear model
- Business logic is explicit and documented
- Terminology is descriptive  
- Single configuration point for dates
- Direct use of core macros (get_observations, get_medication_orders)
- Works with any campaign via flu_current_campaign variable
- Separate from vaccination status tracking (see fct_flu_status)

Usage: 
- Default: Uses flu_current_campaign variable (defaults to flu_2024_25)
- Specific campaign: dbt run --vars '{"flu_current_campaign": "flu_2025_26"}'
- For vaccination tracking, use fct_flu_status instead
- This replaces all the old complex macro-based models
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'person_id', 'campaign_category']
) }}

WITH
-- Age-based eligibility (both campaigns automatically included from intermediate models)
age_based_eligibility AS (
    -- Over 65
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_over_65') }}
    
    UNION ALL
    
    -- Children preschool age
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_children_preschool') }}
    
    UNION ALL
    
    -- Children school age
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'AGE_BASED' AS rule_type, 1 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_children_school_age') }}
    
    UNION ALL
    
    -- Under 65 At Risk (parent category for clinical conditions in under 65s)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'PARENT_CATEGORY' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_under_65_at_risk') }}
),

-- Simple clinical condition eligibility
clinical_condition_eligibility AS (
    -- Chronic Heart Disease
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_chronic_heart_disease') }}
    
    UNION ALL
    
    -- Chronic Liver Disease
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_chronic_liver_disease') }}
    
    UNION ALL
    
    -- Chronic Neurological Disease
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_chronic_neurological_disease') }}
    
    UNION ALL
    
    -- Asplenia
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_asplenia') }}
    
    UNION ALL
    
    -- Learning Disability
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_learning_disability') }}
    
    UNION ALL
    
    -- Household Immunocompromised Contact
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'SOCIAL_FACTOR' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_household_immunocompromised') }}
    
    UNION ALL
    
    -- Asthma Admission (simple rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'CLINICAL_CONDITION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_asthma_admission') }}
),

-- Complex clinical condition eligibility (combination/hierarchical/exclusion rules)
complex_clinical_eligibility AS (
    -- Asthma (combination rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_active_asthma_management') }}
    
    UNION ALL
    
    -- CKD (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_chronic_kidney_disease') }}
    
    UNION ALL
    
    -- Diabetes (exclusion rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'EXCLUSION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_diabetes') }}
    
    UNION ALL
    
    -- Immunosuppression (combination rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_immunosuppression') }}
    
    UNION ALL
    
    -- Health & Social Care Workers (combination rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_health_social_care_worker') }}
    
    UNION ALL
    
    -- Chronic Respiratory Disease (combination rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'COMBINATION' AS rule_type, 3 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_chronic_respiratory_disease') }}
    
    UNION ALL
    
    -- Severe Obesity (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_severe_obesity') }}
    
    UNION ALL
    
    -- Homeless (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 4 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_homeless') }}
    
    UNION ALL
    
    -- Long-term Residential Care (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_long_term_residential_care') }}
    
    UNION ALL
    
    -- Pregnancy (hierarchical rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'HIERARCHICAL' AS rule_type, 2 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_pregnancy') }}
    
    UNION ALL
    
    -- Carer (exclusion rule)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'EXCLUSION' AS rule_type, 5 AS eligibility_priority, created_at
    FROM {{ ref('int_flu_carer') }}
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