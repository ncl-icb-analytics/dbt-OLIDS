/*
Flu Age Birth Range Rules Intermediate Model

Handles birth date range eligibility rules for flu vaccination programme.
These rules are based on specific birth date ranges for child populations.

Age birth range rule groups:
- CHILD_2_3: Children aged 2-3 years (born Sept 2020 - Aug 2022 for 2024-25)
- CHILD_4_16: School children aged 4-16 years (Reception to Year 11, born Sept 2008 - Aug 2020 for 2024-25)

This model replaces the age birth range functionality in the apply_flu_rule macro.
Campaign is configurable via dbt variables.
*/

{{ config(
    materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH age_birth_range_rules AS (
    SELECT 
        l.campaign_id,
        l.rule_group_id,
        l.rule_group_name,
        l.business_description AS description,
        d.birth_start,
        d.birth_end
    FROM {{ ref('stg_flu_programme_logic') }} l
    JOIN {{ ref('stg_flu_campaign_dates') }} d_start
        ON l.campaign_id = d_start.campaign_id
        AND l.rule_group_id = d_start.rule_group_id
        AND d_start.date_type = 'birth_start'
    JOIN {{ ref('stg_flu_campaign_dates') }} d_end
        ON l.campaign_id = d_end.campaign_id
        AND l.rule_group_id = d_end.rule_group_id
        AND d_end.date_type = 'birth_end'
    CROSS JOIN (
        SELECT 
            d_start.date_value AS birth_start,
            d_end.date_value AS birth_end
        FROM {{ ref('stg_flu_campaign_dates') }} d_start
        JOIN {{ ref('stg_flu_campaign_dates') }} d_end
            ON d_start.campaign_id = d_end.campaign_id
            AND d_start.rule_group_id = d_end.rule_group_id
        WHERE d_start.date_type = 'birth_start'
            AND d_end.date_type = 'birth_end'
    ) d
    WHERE l.rule_type = 'AGE_BIRTH_RANGE'
        AND l.campaign_id = '{{ current_campaign }}'
),

campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

eligible_children AS (
    SELECT 
        r.campaign_id,
        r.rule_group_id,
        r.rule_group_name,
        r.description,
        p.person_id,
        p.birth_date_approx,
        cc.reference_date,
        r.birth_start,
        r.birth_end,
        DATEDIFF('month', p.birth_date_approx, cc.reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', p.birth_date_approx, cc.reference_date) AS age_years_at_ref_date
    FROM age_birth_range_rules r
    CROSS JOIN {{ ref('dim_person_demographics') }} p
    CROSS JOIN campaign_config cc
    WHERE p.birth_date_approx IS NOT NULL
        -- Child must be born within the specified birth date range
        AND p.birth_date_approx >= r.birth_start
        AND p.birth_date_approx <= r.birth_end
)

SELECT 
    campaign_id,
    rule_group_id,
    rule_group_name,
    person_id,
    birth_date_approx,
    reference_date,
    birth_start,
    birth_end,
    age_months_at_ref_date AS age_months,
    age_years_at_ref_date AS age_years,
    description,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM eligible_children

ORDER BY rule_group_id, person_id