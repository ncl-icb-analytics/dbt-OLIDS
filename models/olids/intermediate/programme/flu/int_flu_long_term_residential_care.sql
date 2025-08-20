/*
Simplified Long-term Residential Care Eligibility Rule

Business Rule: Person is eligible if they have:
1. Latest residential status code (RESIDE_COD) is a long-term care code (LONGRES_COD)
   - Gets all residential codes and checks if most recent one indicates long-term care
2. AND aged 6 months or over (no upper age limit)

Hierarchical rule - uses latest code logic to determine current residential status.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Get all residential status codes for each person (for all campaigns)
all_residential_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date,
        'RESIDE_COD' AS code_type,
        1 AS is_residential_code,
        cc.audit_end_date
    FROM ({{ get_observations("'RESIDE_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    
    UNION ALL
    
    SELECT 
        cc.campaign_id,
        obs.person_id,
        obs.clinical_effective_date,
        'LONGRES_COD' AS code_type,
        1 AS is_longres_code,
        cc.audit_end_date
    FROM ({{ get_observations("'LONGRES_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
),

-- Step 2: Find latest residential code per person (for all campaigns)
latest_residential_status AS (
    SELECT 
        campaign_id,
        person_id,
        clinical_effective_date AS latest_residential_date,
        code_type AS latest_code_type,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY clinical_effective_date DESC) AS rn
    FROM all_residential_codes
),

-- Step 3: Filter to people whose latest residential code indicates long-term care (for all campaigns)
people_in_long_term_care AS (
    SELECT 
        campaign_id,
        person_id,
        latest_residential_date,
        latest_code_type
    FROM latest_residential_status
    WHERE rn = 1  -- Most recent residential code
        AND latest_code_type = 'LONGRES_COD'  -- Latest code indicates long-term care
),

-- Step 4: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        pltc.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Long Term Residential Care' AS risk_group,
        pltc.person_id,
        pltc.latest_residential_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People living in care homes or long-term residential care' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM people_in_long_term_care pltc
    JOIN all_campaigns cc
        ON pltc.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON pltc.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months or over (no upper age limit)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY campaign_id, person_id