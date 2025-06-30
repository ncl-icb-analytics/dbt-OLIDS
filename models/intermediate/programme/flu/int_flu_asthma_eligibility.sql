/*
=============================================================================
Flu Asthma Eligibility Intermediate Model
=============================================================================

Implements the specific business logic for asthma-related flu vaccination eligibility.
This replaces the apply_asthma_combination_rule macro functionality with explicit
SQL logic for better maintainability and auditability.

BUSINESS LOGIC:
--------------
1. PRIMARY REQUIREMENT: Asthma diagnosis (AST_COD) - uses earliest occurrence
2. SECONDARY REQUIREMENT (AND): Evidence of active asthma management or severity:
   - Recent asthma medication (ASTMED_COD or ASTRX_COD) since specified lookback date
   - OR asthma admission (ASTADM_COD) at any time in patient history

AGE RESTRICTIONS:
----------------
- Minimum: 6 months of age at reference date
- Maximum: Under 65 years of age at reference date

EVIDENCE HIERARCHY:
------------------
When multiple evidence types exist for the same person, the most recent evidence
date is selected to represent the qualifying event.

DATA QUALITY NOTES:
------------------
- All clinical events must occur on or before the campaign reference date
- Medication lookback period is campaign-specific (defined in campaign dates)
- Admission evidence has no time restriction (any historical admission qualifies)
*/

{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true}
) }}

{%- set current_campaign = var('flu_current_campaign') -%}

-- =============================================================================
-- CAMPAIGN CONFIGURATION
-- =============================================================================
-- Extract campaign-specific dates for asthma eligibility rules
WITH campaign_config AS (
    SELECT 
        campaign_id,
        -- Medication lookback date: defines how far back to look for asthma medications
        MAX(CASE WHEN rule_group_id = 'AST_GROUP' AND date_type = 'latest_since_date' THEN date_value END) AS medication_lookback_date,
        -- Reference date: official campaign date for age calculations and event cutoffs
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- =============================================================================
-- PRIMARY REQUIREMENT: ASTHMA DIAGNOSIS
-- =============================================================================
-- Get earliest asthma diagnosis (AST_COD) for each person
-- This establishes the baseline asthma status required for eligibility
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

-- =============================================================================
-- SECONDARY EVIDENCE: RECENT ASTHMA MEDICATIONS
-- =============================================================================
-- Get recent asthma medications (ASTMED_COD/ASTRX_COD) since lookback date
-- This provides evidence of active asthma management within the specified timeframe
recent_asthma_medications AS (
    -- Asthma medication observations (ASTMED_COD)
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
    
    -- Asthma medication orders (ASTRX_COD)
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

-- =============================================================================
-- SECONDARY EVIDENCE: ASTHMA ADMISSIONS
-- =============================================================================
-- Get asthma admissions (ASTADM_COD) at any time in patient history
-- This provides evidence of asthma severity regardless of timing
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

-- =============================================================================
-- EVIDENCE COMBINATION
-- =============================================================================
-- Combine all evidence types (medications and admissions)
-- Each person may have multiple evidence types
asthma_evidence AS (
    -- Recent medication evidence
    SELECT 
        person_id,
        latest_medication_date AS evidence_date,
        'Recent asthma medication' AS evidence_type
    FROM recent_asthma_medications
    
    UNION ALL
    
    -- Admission evidence (any time)
    SELECT 
        person_id,
        latest_admission_date AS evidence_date,
        'Asthma admission' AS evidence_type
    FROM asthma_admissions
),

-- =============================================================================
-- EVIDENCE HIERARCHY
-- =============================================================================
-- Select the most recent evidence per person when multiple evidence types exist
-- This creates a single qualifying evidence record per person
best_evidence AS (
    SELECT 
        person_id,
        evidence_date,
        evidence_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY evidence_date DESC) AS rn
    FROM asthma_evidence
),

-- =============================================================================
-- ELIGIBILITY COMBINATION
-- =============================================================================
-- Combine asthma diagnosis AND evidence to determine final eligibility
-- This implements the core business logic: diagnosis + (medication OR admission)
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
        AND be.rn = 1  -- Only the most recent evidence
    CROSS JOIN campaign_config cc
)

-- =============================================================================
-- FINAL OUTPUT WITH AGE RESTRICTIONS
-- =============================================================================
-- Apply age restrictions and add demographic information
-- Final eligibility requires: asthma diagnosis + evidence + age criteria
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