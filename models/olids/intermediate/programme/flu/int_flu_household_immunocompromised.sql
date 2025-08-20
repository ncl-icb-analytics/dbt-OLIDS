/*
Simplified Household Contact Immunocompromised Eligibility Rule

Business Rule: Person is eligible if they have:
1. A household contact immunocompromised code (HHLD_IMDEF_COD) - latest occurrence in history
2. AND aged 6 months or older (minimum age for flu vaccination)

Simple diagnosis rule - single code with age restrictions.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with household immunocompromised contact codes (for all campaigns)
people_with_household_immunocompromised AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_hhld_imdef_date
    FROM ({{ get_observations("'HHLD_IMDEF_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        hi.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Household Contact Immunocompromised' AS risk_group,
        hi.person_id,
        hi.latest_hhld_imdef_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People living with someone with a weakened immune system' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM people_with_household_immunocompromised hi
    JOIN all_campaigns cc ON hi.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON hi.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months to under 65 years
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY person_id