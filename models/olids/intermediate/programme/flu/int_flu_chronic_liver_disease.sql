/*
Simplified Chronic Liver Disease Eligibility Rule

Business Rule: Person is eligible if they have:
1. A chronic liver disease diagnosis (CLD_COD) - earliest occurrence in history
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

-- Step 1: Find people with chronic liver disease diagnosis (for all campaigns)
people_with_cld_diagnosis AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS first_cld_date,
        cc.audit_end_date
    FROM ({{ get_observations("'CLD_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        cld.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Chronic Liver Disease' AS risk_group,
        cld.person_id,
        cld.first_cld_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'People with chronic liver disease (cirrhosis, hepatitis, etc.)' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cld.audit_end_date AS created_at
    FROM people_with_cld_diagnosis cld
    JOIN all_campaigns cc
        ON cld.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON cld.person_id = demo.person_id
    WHERE 1=1
        -- Apply age restrictions: 6 months or older (minimum age for flu vaccination)
        AND DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) >= 6
)

SELECT * FROM final_eligibility
ORDER BY person_id