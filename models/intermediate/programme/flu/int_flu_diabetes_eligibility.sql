/*
Flu Diabetes Eligibility Intermediate Model

Implements the specific business logic for diabetes-related flu vaccination eligibility.
This replaces the apply_diabetes_exclusion_rule macro functionality.

Business Logic:
- Addison's disease (ADDIS_COD) - earliest occurrence, OR
- Diabetes diagnosis (DIAB_COD) AND either:
  - No diabetes resolved code (DMRES_COD), OR  
  - Latest diabetes diagnosis is more recent than latest resolved code

Age Restrictions: 6 months to 65 years
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

-- Get Addison's disease diagnoses (earliest occurrence)
addisons_diagnoses AS (
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS first_addisons_date,
        'Addison''s disease' AS diagnosis_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'DIAB_GROUP') }})
    WHERE cluster_id = 'ADDIS_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get diabetes diagnoses (latest occurrence)
diabetes_diagnoses AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_diabetes_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'DIAB_GROUP') }})
    WHERE cluster_id = 'DIAB_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get diabetes resolved codes (latest occurrence)
diabetes_resolved AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_resolved_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'DIAB_GROUP') }})
    WHERE cluster_id = 'DMRES_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Determine diabetes eligibility (exclusion logic)
diabetes_eligible AS (
    SELECT 
        dd.person_id,
        dd.latest_diabetes_date AS qualifying_event_date,
        'Diabetes (type 1 or 2)' AS diagnosis_type,
        dr.latest_resolved_date
    FROM diabetes_diagnoses dd
    LEFT JOIN diabetes_resolved dr
        ON dd.person_id = dr.person_id
    WHERE 1=1
        -- Include if no resolved code OR diabetes is more recent than resolved
        AND (dr.latest_resolved_date IS NULL OR dd.latest_diabetes_date > dr.latest_resolved_date)
),

-- Union Addison's and eligible diabetes cases
all_eligible AS (
    -- Addison's disease cases
    SELECT 
        person_id,
        first_addisons_date AS qualifying_event_date,
        diagnosis_type,
        NULL AS latest_resolved_date
    FROM addisons_diagnoses
    
    UNION ALL
    
    -- Diabetes cases (after exclusion logic)
    SELECT 
        person_id,
        qualifying_event_date,
        diagnosis_type,
        latest_resolved_date
    FROM diabetes_eligible
),

-- Add campaign information
diabetes_campaign_eligible AS (
    SELECT 
        cc.campaign_id,
        'DIAB_GROUP' AS rule_group_id,
        'Diabetes' AS rule_group_name,
        ae.person_id,
        ae.qualifying_event_date,
        ae.diagnosis_type,
        ae.latest_resolved_date,
        cc.reference_date,
        'Diabetes (type 1, type 2) or Addison''s disease' AS description
    FROM all_eligible ae
    CROSS JOIN campaign_config cc
)

-- Apply age restrictions and add demographic info
SELECT 
    dce.campaign_id,
    dce.rule_group_id,
    dce.rule_group_name,
    dce.person_id,
    dce.qualifying_event_date,
    dce.diagnosis_type,
    dce.latest_resolved_date,
    dce.reference_date,
    dce.description,
    demo.birth_date,
    DATEDIFF('month', demo.birth_date, dce.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date, dce.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM diabetes_campaign_eligible dce
JOIN {{ ref('dim_person_demographics') }} demo
    ON dce.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 6 months to 65 years (as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date, dce.reference_date) >= 6
    AND DATEDIFF('year', demo.birth_date, dce.reference_date) < 65

ORDER BY person_id