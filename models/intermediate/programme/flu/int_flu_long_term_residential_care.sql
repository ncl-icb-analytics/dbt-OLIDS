/*
Simplified Long-term Residential Care Eligibility Rule

Business Rule: Person is eligible if they have:
1. Latest residential status code (RESIDE_COD) is a long-term care code (LONGRES_COD)
   - Gets all residential codes and checks if most recent one indicates long-term care
2. AND aged 6 months or over (no upper age limit)

Hierarchical rule - uses latest code logic to determine current residential status.
*/

{{ config(materialized='table') }}

{%- set campaign_id = var('flu_current_campaign', 'flu_2024_25') -%}

WITH campaign_config AS (
    {{ flu_campaign_config(campaign_id) }}
),

-- Step 1: Get all residential status codes for each person
all_residential_codes AS (
    SELECT 
        person_id,
        clinical_effective_date,
        'RESIDE_COD' AS code_type,
        1 AS is_residential_code
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    
    UNION ALL
    
    SELECT 
        person_id,
        clinical_effective_date,
        'LONGRES_COD' AS code_type,
        1 AS is_longres_code
    FROM ({{ get_observations("'LONGRES_COD'", 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
),

-- Step 2: Find latest residential code per person
latest_residential_status AS (
    SELECT 
        person_id,
        clinical_effective_date AS latest_residential_date,
        code_type AS latest_code_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) AS rn
    FROM all_residential_codes
),

-- Step 3: Filter to people whose latest residential code indicates long-term care
people_in_long_term_care AS (
    SELECT 
        person_id,
        latest_residential_date,
        latest_code_type
    FROM latest_residential_status
    WHERE rn = 1  -- Most recent residential code
        AND latest_code_type = 'LONGRES_COD'  -- Latest code indicates long-term care
),

-- Step 4: Add demographics and apply age restrictions
final_eligibility AS (
    SELECT 
        '{{ campaign_id }}' AS campaign_id,
        'LONGRES_GROUP' AS rule_group_id,
        'Long Term Residential Care' AS rule_group_name,
        pltc.person_id,
        pltc.latest_residential_date AS qualifying_event_date,
        pltc.latest_code_type,
        cc.campaign_reference_date AS reference_date,
        'People living in care homes or long-term residential care' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM people_in_long_term_care pltc
    CROSS JOIN campaign_config cc
    JOIN {{ ref('dim_person_demographics') }} demo
        ON pltc.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months or over (no upper age limit)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY person_id