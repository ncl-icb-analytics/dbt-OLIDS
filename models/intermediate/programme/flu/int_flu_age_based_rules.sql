/*
Flu Age-Based Rules Intermediate Model

Handles age threshold eligibility rules for flu vaccination programme.
These rules are based purely on age calculations against campaign reference dates.

Age-based rule groups:
- OVER65_GROUP: Everyone aged 65 and over at campaign reference date
- Child age groups use AGE_BIRTH_RANGE rule type (handled separately)

This model replaces the apply_age_based_rule macro functionality.
Campaign is configurable via dbt variables.
*/

{{ config(materialized='table') }}

WITH age_based_rules AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        age_min_months,
        age_max_years,
        description,
        ref_dat,
        audit_end_dat
    FROM {{ ref('stg_flu_programme_rules') }}
    WHERE rule_type = 'AGE_BASED'
        AND campaign_id = '{{ var("flu_current_campaign") }}'
),

eligible_people AS (
    SELECT 
        r.campaign_id,
        r.rule_group_id,
        r.rule_group_name,
        r.description,
        p.person_id,
        p.birth_date,
        r.ref_dat AS reference_date,
        DATEDIFF('month', p.birth_date, r.ref_dat) AS age_months,
        DATEDIFF('year', p.birth_date, r.ref_dat) AS age_years
    FROM age_based_rules r
    CROSS JOIN {{ ref('dim_person_demographics') }} p
    WHERE p.birth_date IS NOT NULL
)

SELECT 
    campaign_id,
    rule_group_id,
    rule_group_name,
    person_id,
    birth_date,
    reference_date,
    age_months,
    age_years,
    description,
    CURRENT_DATE AS created_at
FROM eligible_people
WHERE 1=1
    -- Apply age minimum (if specified)
    AND (age_min_months IS NULL OR age_months >= age_min_months)
    -- Apply age maximum (if specified) 
    AND (age_max_years IS NULL OR age_years < age_max_years)

ORDER BY rule_group_id, person_id