/*
Flu Carer Exclusion Eligibility Intermediate Model

Implements the exclusion business logic for carer-related flu vaccination eligibility.
This follows the same pattern as diabetes exclusion logic.

Business Logic:
- Latest carer code (CARER_COD) AND either:
  - No "not carer" code (NOTCARER_COD), OR  
  - Latest carer code is more recent than latest "not carer" code
- AND person is NOT already eligible under clinical risk groups, BMI, or pregnancy

Age Restrictions: 5-64 years (60 months to 65 years)
*/

{{ config(materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- Get carer codes (latest occurrence)
carer_codes AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_carer_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'CARER_GROUP') }})
    WHERE cluster_id = 'CARER_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get "not carer" codes (latest occurrence)
not_carer_codes AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_not_carer_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'CARER_GROUP') }})
    WHERE cluster_id = 'NOTCARER_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Apply exclusion logic for carer status
carer_eligible AS (
    SELECT 
        cc.person_id,
        cc.latest_carer_date AS qualifying_event_date,
        'Unpaid carer' AS carer_type,
        ncc.latest_not_carer_date
    FROM carer_codes cc
    LEFT JOIN not_carer_codes ncc
        ON cc.person_id = ncc.person_id
    WHERE 1=1
        -- Include if no "not carer" code OR carer is more recent than "not carer"
        AND (ncc.latest_not_carer_date IS NULL OR cc.latest_carer_date > ncc.latest_not_carer_date)
),

-- Get people already eligible under clinical risk groups (for exclusion)
clinical_risk_eligible AS (
    -- Age-based (Over 65)
    SELECT DISTINCT person_id, 'age_based' AS exclusion_reason
    FROM {{ ref('int_flu_age_based_rules') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Simple clinical conditions
    SELECT DISTINCT person_id, 'simple_clinical' AS exclusion_reason
    FROM {{ ref('int_flu_simple_rules') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Combination rules (asthma, respiratory, immunosuppression)
    SELECT DISTINCT person_id, 'combination_clinical' AS exclusion_reason
    FROM {{ ref('int_flu_combination_rules') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Asthma specific
    SELECT DISTINCT person_id, 'asthma' AS exclusion_reason
    FROM {{ ref('int_flu_asthma_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Diabetes specific
    SELECT DISTINCT person_id, 'diabetes' AS exclusion_reason
    FROM {{ ref('int_flu_diabetes_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- BMI (severe obesity)
    SELECT DISTINCT person_id, 'bmi' AS exclusion_reason
    FROM {{ ref('int_flu_bmi_hierarchical_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Pregnancy
    SELECT DISTINCT person_id, 'pregnancy' AS exclusion_reason
    FROM {{ ref('int_flu_pregnancy_hierarchical_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- CKD
    SELECT DISTINCT person_id, 'ckd' AS exclusion_reason
    FROM {{ ref('int_flu_ckd_hierarchical_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Remaining simple groups
    SELECT DISTINCT person_id, 'remaining_simple' AS exclusion_reason
    FROM {{ ref('int_flu_remaining_simple_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
    
    UNION
    
    -- Remaining combination groups
    SELECT DISTINCT person_id, 'remaining_combination' AS exclusion_reason
    FROM {{ ref('int_flu_remaining_combination_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
),

-- Apply exclusion: remove carers already eligible under other groups
carer_final_eligible AS (
    SELECT 
        ce.person_id,
        ce.qualifying_event_date,
        ce.carer_type,
        ce.latest_not_carer_date
    FROM carer_eligible ce
    LEFT JOIN clinical_risk_eligible cre
        ON ce.person_id = cre.person_id
    WHERE cre.person_id IS NULL  -- NOT in clinical risk groups
),

-- Add campaign information
carer_campaign_eligible AS (
    SELECT 
        cc.campaign_id,
        'CARER_GROUP' AS rule_group_id,
        'Carer' AS rule_group_name,
        cfe.person_id,
        cfe.qualifying_event_date,
        cfe.carer_type,
        cfe.latest_not_carer_date,
        cc.reference_date,
        'Unpaid carers aged 5-64 (not already eligible for other reasons)' AS description
    FROM carer_final_eligible cfe
    CROSS JOIN campaign_config cc
)

-- Apply age restrictions and add demographic info
SELECT 
    cce.campaign_id,
    cce.rule_group_id,
    cce.rule_group_name,
    cce.person_id,
    cce.qualifying_event_date,
    cce.carer_type,
    cce.latest_not_carer_date,
    cce.reference_date,
    cce.description,
    demo.birth_date_approx,
    DATEDIFF('month', demo.birth_date_approx, cce.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date_approx, cce.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM carer_campaign_eligible cce
JOIN {{ ref('dim_person_demographics') }} demo
    ON cce.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 5-64 years (60 months to 65 years, as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date_approx, cce.reference_date) >= 60   -- 5 years
    AND DATEDIFF('year', demo.birth_date_approx, cce.reference_date) < 65

ORDER BY person_id