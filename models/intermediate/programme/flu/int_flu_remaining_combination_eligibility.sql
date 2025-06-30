/*
Flu Remaining Combination Rules Eligibility Intermediate Model

Implements combination eligibility rules that require multiple conditions using AND/OR logic.

Rule Groups Covered:
- IMMUNO_GROUP: Immunosuppression (diagnosis OR medication OR administration OR chemotherapy)
- RESP_GROUP: Chronic respiratory disease (asthma groups OR respiratory diagnosis)

This model implements the proper combination logic as defined in flu_programme_logic.csv.
*/

{{ config(materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date,
        MAX(CASE WHEN rule_group_id = 'IMMUNO_GROUP' AND date_type = 'latest_since_date' THEN date_value END) AS immuno_lookback_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- IMMUNO_GROUP: Immunosuppression diagnosis
immuno_diagnosis AS (
    SELECT DISTINCT
        person_id,
        MAX(clinical_effective_date) AS latest_diagnosis_date,
        'Immunosuppression diagnosis' AS evidence_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'IMMUNO_GROUP') }})
    WHERE cluster_id = 'IMMDX_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- IMMUNO_GROUP: Immunosuppression medication (since lookback date)
immuno_medication AS (
    SELECT DISTINCT
        med.person_id,
        MAX(med.order_date) AS latest_medication_date,
        'Immunosuppression medication' AS evidence_type
    FROM ({{ get_flu_medications_for_rule_group(current_campaign, 'IMMUNO_GROUP') }}) med
    CROSS JOIN campaign_config cc
    WHERE med.cluster_id = 'IMMRX_COD'
        AND med.order_date IS NOT NULL
        AND med.order_date >= cc.immuno_lookback_date
        AND med.order_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY med.person_id
),

-- IMMUNO_GROUP: Immunosuppression administration (since lookback date)
immuno_administration AS (
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_admin_date,
        'Immunosuppression administration' AS evidence_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'IMMUNO_GROUP') }}) obs
    CROSS JOIN campaign_config cc
    WHERE obs.cluster_id = 'IMMADM_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_lookback_date
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
),

-- IMMUNO_GROUP: Chemotherapy/radiotherapy (since lookback date)
immuno_chemotherapy AS (
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_chemo_date,
        'Chemotherapy/radiotherapy' AS evidence_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'IMMUNO_GROUP') }}) obs
    CROSS JOIN campaign_config cc
    WHERE obs.cluster_id = 'DXT_CHEMO_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.immuno_lookback_date
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
),

-- IMMUNO_GROUP: Union all immunosuppression evidence
immuno_all_evidence AS (
    SELECT person_id, latest_diagnosis_date AS evidence_date, evidence_type FROM immuno_diagnosis
    UNION ALL
    SELECT person_id, latest_medication_date AS evidence_date, evidence_type FROM immuno_medication
    UNION ALL
    SELECT person_id, latest_admin_date AS evidence_date, evidence_type FROM immuno_administration
    UNION ALL
    SELECT person_id, latest_chemo_date AS evidence_date, evidence_type FROM immuno_chemotherapy
),

-- IMMUNO_GROUP: Get best evidence per person
immuno_best_evidence AS (
    SELECT 
        person_id,
        evidence_date,
        evidence_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY evidence_date DESC, evidence_type) AS rn
    FROM immuno_all_evidence
    WHERE evidence_date IS NOT NULL
),

-- IMMUNO_GROUP: Final eligibility
immuno_eligibility AS (
    SELECT 
        '{{ current_campaign }}' AS campaign_id,
        'IMMUNO_GROUP' AS rule_group_id,
        'Immunosuppression' AS rule_group_name,
        person_id,
        evidence_date AS qualifying_event_date,
        evidence_type,
        'Immunosuppression (diagnosis, medication, or treatment)' AS description
    FROM immuno_best_evidence
    WHERE rn = 1
),

-- RESP_GROUP: Include people from asthma groups (reference existing asthma models)
resp_from_asthma AS (
    SELECT 
        '{{ current_campaign }}' AS campaign_id,
        'RESP_GROUP' AS rule_group_id,
        'Chronic Respiratory Disease' AS rule_group_name,
        person_id,
        qualifying_event_date,
        'From asthma eligibility' AS evidence_type,
        'Chronic respiratory disease (including asthma)' AS description
    FROM {{ ref('int_flu_asthma_eligibility') }}
    WHERE campaign_id = '{{ current_campaign }}'
),

-- RESP_GROUP: Include people with chronic respiratory disease diagnosis
resp_from_diagnosis AS (
    SELECT 
        '{{ current_campaign }}' AS campaign_id,
        'RESP_GROUP' AS rule_group_id,
        'Chronic Respiratory Disease' AS rule_group_name,
        obs.person_id,
        MIN(obs.clinical_effective_date) AS qualifying_event_date,  -- EARLIEST
        'Chronic respiratory diagnosis' AS evidence_type,
        'Chronic respiratory disease (COPD, cystic fibrosis, etc.)' AS description
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'RESP_GROUP') }}) obs
    WHERE obs.cluster_id = 'RESP_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
),

-- RESP_GROUP: Union respiratory evidence (remove duplicates later)
resp_all_evidence AS (
    SELECT * FROM resp_from_asthma
    UNION ALL
    SELECT * FROM resp_from_diagnosis
),

-- RESP_GROUP: Remove duplicates and get best evidence per person
resp_best_evidence AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        evidence_type,
        description,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY qualifying_event_date DESC, evidence_type) AS rn
    FROM resp_all_evidence
),

-- Final respiratory eligibility
resp_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        evidence_type,
        description
    FROM resp_best_evidence
    WHERE rn = 1
),

-- Union all combination eligibility
all_combination_eligibility AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        evidence_type,
        description
    FROM immuno_eligibility
    
    UNION ALL
    
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        person_id,
        qualifying_event_date,
        evidence_type,
        description
    FROM resp_eligibility
)

-- Apply age restrictions and add demographic info
SELECT 
    ace.campaign_id,
    ace.rule_group_id,
    ace.rule_group_name,
    ace.person_id,
    ace.qualifying_event_date,
    ace.evidence_type,
    cc.reference_date,
    ace.description,
    demo.birth_date,
    DATEDIFF('month', demo.birth_date, cc.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date, cc.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM all_combination_eligibility ace
CROSS JOIN campaign_config cc
JOIN {{ ref('dim_person_demographics') }} demo
    ON ace.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 6 months to 65 years (as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date, cc.reference_date) >= 6
    AND DATEDIFF('year', demo.birth_date, cc.reference_date) < 65

ORDER BY rule_group_id, person_id