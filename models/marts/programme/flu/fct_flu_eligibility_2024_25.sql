/*
Flu Vaccination Eligibility Fact Table - 2024/25 Campaign

This unified fact model brings together all flu vaccination eligibility rules 
for the 2024-25 campaign. It replaces the complex macro-based approach with 
a clear, testable model hierarchy.

The model unions eligibility from different rule types:
- Age-based rules (Over 65)
- Simple clinical condition rules (CHD, Learning Disability, etc.)
- Combination rules (Asthma, Respiratory, Immunosuppression)
- Specific rule group models (detailed business logic)

Each person may be eligible under multiple rules - this is expected and correct.
The final output shows all applicable eligibility reasons per person.
*/

/*
Flu Vaccination Eligibility Fact Table - 2024/25 Campaign

This campaign-specific fact model brings together all flu vaccination eligibility 
rules for the 2024-25 campaign specifically. This approach provides:
- Stable, campaign-specific models for analysis
- Clear historical preservation
- Easy analyst experience with concrete model references

For dynamic/comparative analysis, see fct_flu_eligibility_comparison.sql
*/

{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true},
    cluster_by=['campaign_id', 'person_id', 'rule_group_id']
) }}

{%- set campaign_id = 'flu_2024_25' -%}

WITH campaign_config AS (
    SELECT 
        '{{ campaign_id }}' AS campaign_id,
        '2024-25 Flu Vaccination Campaign' AS campaign_name,
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
        birth_date,
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
        birth_date,
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
        birth_date,
        age_months,
        age_years,
        'COMBINATION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_combination_rules') }}
    WHERE campaign_id = '{{ campaign_id }}'
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
        birth_date,
        age_months,
        age_years,
        'AGE_BIRTH_RANGE' AS rule_type,
        created_at
    FROM {{ ref('int_flu_age_birth_range_rules') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Hierarchical rule eligibility
ckd_hierarchical_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'HIERARCHICAL' AS rule_type,
        created_at
    FROM {{ ref('int_flu_ckd_hierarchical_eligibility') }}
),

bmi_hierarchical_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'HIERARCHICAL' AS rule_type,
        created_at
    FROM {{ ref('int_flu_bmi_hierarchical_eligibility') }}
),

pregnancy_hierarchical_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'HIERARCHICAL' AS rule_type,
        created_at
    FROM {{ ref('int_flu_pregnancy_hierarchical_eligibility') }}
),

-- Remaining simple rule eligibility
remaining_simple_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'SIMPLE' AS rule_type,
        created_at
    FROM {{ ref('int_flu_remaining_simple_eligibility') }}
    WHERE campaign_id = '{{ campaign_id }}'
),

-- Remaining combination rule eligibility
remaining_combination_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'COMBINATION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_remaining_combination_eligibility') }}
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
        birth_date,
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
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'EXCLUSION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_diabetes_eligibility') }}
),

-- Carer exclusion eligibility
carer_exclusion_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        reference_date,
        description,
        birth_date,
        age_months_at_ref_date AS age_months,
        age_years_at_ref_date AS age_years,
        'EXCLUSION' AS rule_type,
        created_at
    FROM {{ ref('int_flu_carer_exclusion_eligibility') }}
),

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
    SELECT * FROM ckd_hierarchical_eligibility
    UNION ALL
    SELECT * FROM bmi_hierarchical_eligibility
    UNION ALL
    SELECT * FROM pregnancy_hierarchical_eligibility
    UNION ALL
    SELECT * FROM remaining_simple_eligibility
    UNION ALL
    SELECT * FROM remaining_combination_eligibility
    UNION ALL
    SELECT * FROM asthma_eligibility
    UNION ALL
    SELECT * FROM diabetes_eligibility
    UNION ALL
    SELECT * FROM carer_exclusion_eligibility
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
        ae.birth_date,
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
    birth_date,
    age_months,
    age_years,
    created_at,
    
    -- Add helpful calculated fields
    CASE 
        WHEN qualifying_event_date IS NOT NULL 
        THEN DATEDIFF('days', qualifying_event_date, audit_end_date)
        ELSE NULL
    END AS days_since_qualifying_event,
    
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