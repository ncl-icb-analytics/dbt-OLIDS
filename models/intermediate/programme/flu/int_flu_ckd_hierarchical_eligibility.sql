/*
Flu CKD Hierarchical Eligibility Intermediate Model

Implements the complex hierarchical business logic for CKD-related flu vaccination eligibility.
This replaces the apply_ckd_hierarchical_rule macro functionality.

Business Logic:
- CKD diagnosis (CKD_COD) - earliest occurrence, OR
- Latest CKD stage 3-5 code (CKD35_COD) is more recent than or equal to latest any-stage CKD code (CKD15_COD)

The hierarchy logic ensures that people with more recent severe CKD stages are included,
even if they have older general CKD codes.

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

-- Get CKD diagnoses (earliest occurrence)
ckd_diagnoses AS (
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS first_ckd_date,
        'CKD diagnosis' AS diagnosis_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'CKD_GROUP') }})
    WHERE cluster_id = 'CKD_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get CKD stage 1-5 codes (latest occurrence)
ckd_any_stage AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_any_stage_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'CKD_GROUP') }})
    WHERE cluster_id = 'CKD15_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get CKD stage 3-5 codes (latest occurrence)
ckd_severe_stage AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_severe_stage_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'CKD_GROUP') }})
    WHERE cluster_id = 'CKD35_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Apply hierarchical logic
ckd_eligible AS (
    -- Include people with CKD diagnosis
    SELECT 
        person_id,
        first_ckd_date AS qualifying_event_date,
        diagnosis_type,
        NULL AS stage_hierarchy_note
    FROM ckd_diagnoses
    
    UNION
    
    -- Include people where latest severe stage >= latest any stage  
    SELECT 
        ss.person_id,
        ss.latest_severe_stage_date AS qualifying_event_date,
        'CKD stage 3-5 (hierarchical)' AS diagnosis_type,
        'Latest severe stage (' || ss.latest_severe_stage_date || ') >= latest any stage (' || COALESCE(ans.latest_any_stage_date::VARCHAR, 'none') || ')' AS stage_hierarchy_note
    FROM ckd_severe_stage ss
    LEFT JOIN ckd_any_stage ans
        ON ss.person_id = ans.person_id
    WHERE ss.latest_severe_stage_date >= COALESCE(ans.latest_any_stage_date, ss.latest_severe_stage_date)
),

-- Remove duplicates and get the best qualifying event per person
best_ckd_eligible AS (
    SELECT 
        person_id,
        qualifying_event_date,
        diagnosis_type,
        stage_hierarchy_note,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY qualifying_event_date DESC, diagnosis_type) AS rn
    FROM ckd_eligible
),

-- Add campaign information
ckd_campaign_eligible AS (
    SELECT 
        cc.campaign_id,
        'CKD_GROUP' AS rule_group_id,
        'Chronic Kidney Disease' AS rule_group_name,
        bce.person_id,
        bce.qualifying_event_date,
        bce.diagnosis_type,
        bce.stage_hierarchy_note,
        cc.reference_date,
        'Chronic kidney disease (stage 3-5)' AS description
    FROM best_ckd_eligible bce
    CROSS JOIN campaign_config cc
    WHERE bce.rn = 1
)

-- Apply age restrictions and add demographic info
SELECT 
    cce.campaign_id,
    cce.rule_group_id,
    cce.rule_group_name,
    cce.person_id,
    cce.qualifying_event_date,
    cce.diagnosis_type,
    cce.stage_hierarchy_note,
    cce.reference_date,
    cce.description,
    demo.birth_date_approx,
    DATEDIFF('month', demo.birth_date_approx, cce.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date_approx, cce.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM ckd_campaign_eligible cce
JOIN {{ ref('dim_person_demographics') }} demo
    ON cce.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 6 months to 65 years (as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date_approx, cce.reference_date) >= 6
    AND DATEDIFF('year', demo.birth_date_approx, cce.reference_date) < 65

ORDER BY person_id