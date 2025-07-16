/*
Flu Vaccination Eligibility Fact Table - TEMPLATE

This is a template for creating new campaign-specific flu eligibility models.
Copy this file and replace placeholders to create a new campaign year.

INSTRUCTIONS TO CREATE NEW CAMPAIGN:
1. Copy this file to fct_flu_eligibility_YYYY_YY.sql (e.g., fct_flu_eligibility_2025_26.sql)
2. Replace CAMPAIGN_ID_PLACEHOLDER with actual campaign ID (e.g., 'flu_2025_26')
3. Replace CAMPAIGN_NAME_PLACEHOLDER with descriptive name (e.g., '2025-26 Flu Vaccination Campaign')
4. Add new CSV data for the campaign to the seed files:
   - flu_campaign_dates.csv: Copy 2024-25 rows, update campaign_id and shift dates +1 year
   - flu_programme_logic.csv: Copy 2024-25 rows, update campaign_id, review business logic
   - flu_code_clusters.csv: Usually no changes needed unless UKHSA updates codes
5. Update fct_flu_eligibility_comparison.sql to include the new campaign

The template uses the same structure as existing models but with placeholder campaign ID.
All business logic is driven by the CSV configuration data.
*/

{# 
=== TEMPLATE PARAMETERS ===
Replace these placeholders when creating a new campaign model:
#}
{%- set campaign_id = 'CAMPAIGN_ID_PLACEHOLDER' -%}  {# REPLACE: e.g., 'flu_2025_26' #}

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'person_id', 'rule_group_id']) }}

{# 
=== CAMPAIGN CONFIGURATION ===
This CTE sets up the campaign parameters and dates for the template
#}
WITH campaign_config AS (
    SELECT 
        '{{ campaign_id }}' AS campaign_id,
        'CAMPAIGN_NAME_PLACEHOLDER' AS campaign_name,  {# REPLACE: e.g., '2025-26 Flu Vaccination Campaign' #}
        {{ get_flu_campaign_date(campaign_id, 'ALL', 'start_dat') }}::DATE AS campaign_start_date,
        {{ get_flu_campaign_date(campaign_id, 'ALL', 'ref_dat') }}::DATE AS campaign_ref_date,
        {{ get_flu_audit_date(campaign_id) }} AS audit_end_date
),

-- Age-based eligibility (Over 65)
age_based_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        NULL AS qualifying_event_date,  -- No specific event for age-based
        reference_date,
        description,
        birth_date_approx,
        age_months,
        age_years,
        'AGE_BASED' AS rule_type,
        created_at
    FROM {{ ref('int_flu_age_based_rules') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Simple rule eligibility (single conditions)
simple_rule_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months,
        age_years,
        'SIMPLE' AS rule_type,
        created_at
    FROM {{ ref('int_flu_simple_rules') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Combination rule eligibility 
combination_rule_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months,
        age_years,
        'COMBINATION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_combination_rules') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Specific rule group models (detailed business logic)
asthma_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'COMBINATION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_asthma_eligibility') }}
),

diabetes_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'EXCLUSION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_diabetes_eligibility') }}
),

-- Age birth range eligibility (children)
age_birth_range_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        NULL AS qualifying_event_date,  -- No specific event for age-based
        reference_date,
        description,
        birth_date_approx,
        age_months,
        age_years,
        'AGE_BIRTH_RANGE' AS rule_type,
        created_at
    FROM {{ ref('int_flu_age_birth_range_rules') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Hierarchical rule eligibility (CKD, BMI, pregnancy)
hierarchical_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'HIERARCHICAL' AS rule_type,
        created_at
    FROM {{ ref('int_flu_ckd_hierarchical_eligibility') }}
    
    UNION ALL
    
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'HIERARCHICAL' AS rule_type,
        created_at
    FROM {{ ref('int_flu_bmi_hierarchical_eligibility') }}
    
    UNION ALL
    
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'HIERARCHICAL' AS rule_type,
        created_at
    FROM {{ ref('int_flu_pregnancy_hierarchical_eligibility') }}
),

-- Remaining rule groups
remaining_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'SIMPLE' AS rule_type,
        created_at
    FROM {{ ref('int_flu_remaining_simple_eligibility') }}
    WHERE campaign_id = '{{ campaign_id }}'
    
    UNION ALL
    
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'COMBINATION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_remaining_combination_eligibility') }}
    WHERE campaign_id = '{{ campaign_id }}'
    
    UNION ALL
    
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date_approx,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'EXCLUSION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_carer_exclusion_eligibility') }}
),

{# 
=== ELIGIBILITY CONSOLIDATION ===
Union all eligibility sources into a single dataset.
Each CTE above handles a different type of eligibility rule.
#}
-- Union all eligibility sources
all_eligibility AS (
    SELECT * FROM age_based_eligibility
    UNION ALL
    SELECT * FROM simple_rule_eligibility
    UNION ALL  
    SELECT * FROM combination_rule_eligibility
    UNION ALL
    SELECT * FROM age_birth_range_eligibility
    UNION ALL
    SELECT * FROM hierarchical_eligibility
    UNION ALL
    SELECT * FROM remaining_eligibility
    UNION ALL
    SELECT * FROM asthma_eligibility
    UNION ALL
    SELECT * FROM diabetes_eligibility
),

-- Add campaign context and final formatting
final_eligibility AS (
    SELECT 
        cc.campaign_id,
        cc.campaign_name,
        cc.campaign_start_date,
        cc.campaign_ref_date,
        cc.audit_end_date,
        ae.rule_group_id,
        ae.rule_group_name,
        ae.rule_type,
        ae.person_id,
        ae.qualifying_event_date,
        ae.reference_date,
        ae.description AS eligibility_reason,
        ae.birth_date_approx,
        ae.age_months,
        ae.age_years,
        ae.created_at
    FROM all_eligibility ae
    CROSS JOIN campaign_config cc
)

SELECT 
    campaign_id,
    campaign_name,
    campaign_start_date,
    campaign_ref_date,
    audit_end_date,
    rule_group_id,
    rule_group_name,
    rule_type,
    person_id,
    qualifying_event_date,
    reference_date,
    eligibility_reason,
    birth_date_approx,
    age_months,
    age_years,
    created_at,
    
    -- Add helpful calculated fields
    
    -- Priority scoring for multiple eligibilities (lower = higher priority)
    CASE rule_type
        WHEN 'AGE_BASED' THEN 1           -- Age-based has highest priority
        WHEN 'AGE_BIRTH_RANGE' THEN 2     -- Child age groups
        WHEN 'HIERARCHICAL' THEN 3        -- Complex hierarchical conditions
        WHEN 'COMBINATION' THEN 4         -- Complex combination conditions
        WHEN 'EXCLUSION' THEN 5           -- Exclusion logic (carers, diabetes)
        WHEN 'SIMPLE' THEN 6              -- Simple conditions lowest
        ELSE 7
    END AS eligibility_priority

FROM final_eligibility

ORDER BY person_id, eligibility_priority, rule_group_id