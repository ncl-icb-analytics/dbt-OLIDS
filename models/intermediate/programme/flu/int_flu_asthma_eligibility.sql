/*
Flu Asthma Eligibility Intermediate Model

Implements the specific business logic for asthma-related flu vaccination eligibility.
This replaces the apply_asthma_combination_rule macro functionality.

Business Logic:
- Asthma diagnosis (AST_COD) - earliest occurrence
- AND one of:
  - Recent asthma medication (ASTMED_COD or ASTRX_COD) since specified lookback date
  - OR asthma admission (ASTADM_COD) ever

Age Restrictions: 6 months to 65 years
*/

{{ config(materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'AST_GROUP' AND date_type = 'latest_since_date' THEN date_value END) AS medication_lookback_date,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- Get asthma diagnoses (earliest occurrence)
asthma_diagnoses AS (
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS first_asthma_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'AST_GROUP') }})
    WHERE cluster_id = 'AST_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get recent asthma medications (since lookback date)
recent_asthma_medications AS (
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_medication_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'AST_GROUP') }}) obs
    CROSS JOIN campaign_config cc
    WHERE obs.cluster_id = 'ASTMED_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.medication_lookback_date
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
    
    UNION
    
    SELECT DISTINCT
        med.person_id,
        MAX(med.order_date) AS latest_medication_date  
    FROM ({{ get_flu_medications_for_rule_group(current_campaign, 'AST_GROUP') }}) med
    CROSS JOIN campaign_config cc
    WHERE med.cluster_id = 'ASTRX_COD'
        AND med.order_date IS NOT NULL
        AND med.order_date >= cc.medication_lookback_date
        AND med.order_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY med.person_id
),

-- Get asthma admissions (any time)
asthma_admissions AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_admission_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'AST_ADM_GROUP') }})
    WHERE cluster_id = 'ASTADM_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Combine medication and admission evidence
asthma_evidence AS (
    SELECT 
        person_id,
        latest_medication_date AS evidence_date,
        'Recent asthma medication' AS evidence_type
    FROM recent_asthma_medications
    
    UNION ALL
    
    SELECT 
        person_id,
        latest_admission_date AS evidence_date,
        'Asthma admission' AS evidence_type
    FROM asthma_admissions
),

-- Get the best evidence per person (most recent)
best_evidence AS (
    SELECT 
        person_id,
        evidence_date,
        evidence_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY evidence_date DESC) AS rn
    FROM asthma_evidence
),

-- Final eligibility: asthma diagnosis AND evidence
asthma_eligible AS (
    SELECT 
        cc.campaign_id,
        'AST_GROUP' AS rule_group_id,
        'Asthma' AS rule_group_name,
        ad.person_id,
        ad.first_asthma_date AS qualifying_event_date,
        be.evidence_date,
        be.evidence_type,
        cc.reference_date,
        'Asthma diagnosis with recent medication or admission' AS description
    FROM asthma_diagnoses ad
    JOIN best_evidence be
        ON ad.person_id = be.person_id
        AND be.rn = 1
    CROSS JOIN campaign_config cc
)

-- Apply age restrictions and add demographic info
SELECT 
    ae.campaign_id,
    ae.rule_group_id,
    ae.rule_group_name,
    ae.person_id,
    ae.qualifying_event_date,
    ae.evidence_date,
    ae.evidence_type,
    ae.reference_date,
    ae.description,
    demo.birth_date,
    DATEDIFF('month', demo.birth_date, ae.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date, ae.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM asthma_eligible ae
JOIN {{ ref('dim_person_demographics') }} demo
    ON ae.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 6 months to 65 years (as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date, ae.reference_date) >= 6
    AND DATEDIFF('year', demo.birth_date, ae.reference_date) < 65

ORDER BY person_id