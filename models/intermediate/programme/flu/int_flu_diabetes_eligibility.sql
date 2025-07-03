/*
Flu Diabetes Eligibility Intermediate Model

Implements diabetes and Addison's disease eligibility logic for flu vaccination program.
These conditions qualify patients due to increased risk of flu complications from immunocompromise.

CLINICAL ELIGIBILITY CRITERIA:
1. Addison's Disease (ADDIS_COD):
   - Any diagnosis qualifies (earliest occurrence used)
   - Adrenal insufficiency causes immunocompromise
   
2. Diabetes Type 1/2 (DIAB_COD):
   - Must have active diabetes diagnosis
   - Excludes resolved diabetes using hierarchical date logic:
     * Include if NO diabetes resolved code (DMRES_COD), OR
     * Include if latest diabetes diagnosis > latest resolved code
   - Higher infection risk due to hyperglycemia effects on immune system

AGE RESTRICTIONS:
- Minimum: 6 months (infants need maternal antibodies first)
- Maximum: Under 65 years (over 65s eligible via separate age rule)

This model replaces the apply_diabetes_exclusion_rule macro functionality.
*/

{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true},
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Flu Diabetes Eligibility - Diabetes and Addison disease eligibility determination for NHS flu vaccination programme.

Clinical Purpose:
• NHS flu vaccination programme diabetes and Addison disease eligibility assessment
• High-risk population identification due to immunocompromise and infection risk
• Clinical eligibility validation for diabetes and adrenal insufficiency conditions
• Age-restricted eligibility determination (6 months to under 65 years)

Data Granularity:
• One row per person meeting diabetes or Addison disease eligibility criteria
• Uses hierarchical date logic for active diabetes determination
• Excludes resolved diabetes using latest diagnosis vs resolution comparison

Key Features:
• Active diabetes determination with resolution exclusion logic
• Addison disease immunocompromise risk assessment
• Age restriction compliance (6 months to under 65 years)
• Essential for high-risk population vaccination targeting'"
    ]
) }}

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
-- Addison's patients are immunocompromised due to adrenal insufficiency
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

-- Get diabetes diagnoses (latest occurrence for most recent clinical status)
-- Type 1 and Type 2 diabetes increase flu complication risk via immunocompromise
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

-- Get diabetes resolved codes (latest occurrence for exclusion logic)
-- Some patients may have diabetes recorded as resolved (e.g., gestational diabetes)
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

-- Apply diabetes exclusion logic - only include patients with active diabetes
-- Business rule: diabetes must be more recent than any resolved code
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
        -- Include if no resolved code OR diabetes diagnosis is more recent than resolved
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

-- Apply clinical age restrictions and add demographic context
-- Age limits ensure appropriate targeting within diabetes/Addison's eligibility
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
    demo.birth_date_approx,
    DATEDIFF('month', demo.birth_date_approx, dce.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date_approx, dce.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM diabetes_campaign_eligible dce
JOIN {{ ref('dim_person_demographics') }} demo
    ON dce.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 6 months minimum (infants rely on maternal antibodies)
    AND DATEDIFF('month', demo.birth_date_approx, dce.reference_date) >= 6
    -- Age restrictions: Under 65 years (over 65s covered by separate age-based rule)
    AND DATEDIFF('year', demo.birth_date_approx, dce.reference_date) < 65

ORDER BY person_id