{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Person-level alcohol status combining AUDIT scores and disorder diagnoses.
Provides comprehensive alcohol risk assessment for population health management.

Business Logic:
1. Uses most recent AUDIT/AUDIT-C score as primary indicator
2. Considers alcohol disorder diagnoses for additional context
3. Recent AUDIT scores override older disorder codes (unless disorder is more recent)
4. Applies 12-month recency window for conflicting assessments

This is PERSON-LEVEL data - one row per person with any alcohol-related data.
*/

WITH latest_audit_scores AS (
    -- Get the most recent AUDIT score for each person
    SELECT 
        person_id,
        clinical_effective_date AS latest_audit_date,
        audit_score,
        audit_type,
        risk_category,
        simplified_risk_level,
        auditc_positive_screen,
        requires_specialist_intervention,
        brief_intervention_indicated,
        sex
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY person_id 
                ORDER BY clinical_effective_date DESC, 
                         CASE WHEN audit_type = 'Full AUDIT' THEN 1 ELSE 2 END -- Prefer full AUDIT if same date
            ) AS rn
        FROM {{ ref('int_alcohol_audit_scores') }}
        WHERE is_valid_score = TRUE
    )
    WHERE rn = 1
),

alcohol_disorders AS (
    -- Get alcohol disorder history - basic summary
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_disorder_date,
        MAX(CASE WHEN is_problem = TRUE AND (problem_end_date IS NULL OR problem_end_date > CURRENT_DATE()) 
                 THEN clinical_effective_date END) AS latest_active_disorder_date,
        
        -- Basic flags without interpretation
        BOOLOR_AGG(is_problem = TRUE AND (problem_end_date IS NULL OR problem_end_date > CURRENT_DATE())) AS has_active_disorder,
        
        COUNT(DISTINCT observation_id) AS disorder_record_count
    FROM {{ ref('int_alcohol_misuse_disorders') }}
    GROUP BY person_id
),

combined_data AS (
    -- Combine AUDIT scores and disorder diagnoses
    SELECT 
        COALESCE(a.person_id, d.person_id) AS person_id,
        a.sex,
        
        -- AUDIT information
        a.latest_audit_date,
        a.audit_score,
        a.audit_type,
        a.risk_category AS audit_risk_category,
        a.simplified_risk_level AS audit_simplified_risk,
        a.auditc_positive_screen,
        a.requires_specialist_intervention AS audit_requires_intervention,
        a.brief_intervention_indicated,
        
        -- Disorder information
        d.latest_disorder_date,
        d.latest_active_disorder_date,
        d.has_active_disorder,
        d.disorder_record_count,
        
        -- Calculate recency
        DATEDIFF(DAY, a.latest_audit_date, CURRENT_DATE()) AS days_since_audit,
        DATEDIFF(DAY, d.latest_disorder_date, CURRENT_DATE()) AS days_since_disorder
        
    FROM latest_audit_scores a
    FULL OUTER JOIN alcohol_disorders d
        ON a.person_id = d.person_id
)

SELECT 
    person_id,
    sex,
    
    -- Assessment dates
    latest_audit_date,
    latest_disorder_date,
    latest_active_disorder_date,
    GREATEST(latest_audit_date, latest_disorder_date) AS latest_assessment_date,
    
    -- AUDIT details
    audit_score,
    audit_type,
    audit_risk_category,
    audit_simplified_risk,
    auditc_positive_screen,
    brief_intervention_indicated,
    
    -- Disorder details
    has_active_disorder,
    disorder_record_count,
    
    -- Recency indicators
    days_since_audit,
    days_since_disorder,
    
    -- Primary alcohol status (disorder status takes priority if present)
    CASE
        WHEN has_active_disorder THEN 'Active Alcohol Misuse Disorder'
        WHEN disorder_record_count > 0 THEN 'Historical Alcohol Misuse Disorder'
        WHEN audit_risk_category IS NOT NULL THEN audit_risk_category
        ELSE NULL
    END AS alcohol_status,
    
    -- Risk sort key (must exactly match the alcohol_status logic precedence)
    CASE
        WHEN has_active_disorder THEN 7  -- Active Alcohol Misuse Disorder (highest risk)
        WHEN disorder_record_count > 0 THEN 4  -- Historical Alcohol Misuse Disorder (moderate risk)
        WHEN audit_risk_category = 'Possible Dependence' THEN 6
        WHEN audit_risk_category = 'Higher Risk' THEN 5
        WHEN audit_risk_category = 'Increasing Risk' THEN 3
        WHEN audit_risk_category IN ('Lower Risk', 'Occasional Drinker') THEN 2
        WHEN audit_risk_category = 'Non-Drinker' THEN 1
        ELSE 0  -- Unknown/No data
    END AS alcohol_risk_sort_key,
    
    -- Intervention flag
    CASE
        WHEN brief_intervention_indicated = TRUE OR has_active_disorder = TRUE THEN TRUE
        ELSE FALSE
    END AS requires_intervention,
    
    -- Data quality flags
    CASE
        WHEN latest_audit_date IS NOT NULL AND days_since_audit <= 365 THEN 'Current'
        WHEN latest_audit_date IS NOT NULL AND days_since_audit <= 730 THEN 'Recent'
        WHEN latest_audit_date IS NOT NULL THEN 'Historical'
        ELSE 'No AUDIT'
    END AS audit_data_recency,
    
    CASE
        WHEN latest_disorder_date IS NOT NULL AND days_since_disorder <= 365 THEN 'Current'
        WHEN latest_disorder_date IS NOT NULL AND days_since_disorder <= 730 THEN 'Recent'  
        WHEN latest_disorder_date IS NOT NULL THEN 'Historical'
        ELSE 'No Disorder Record'
    END AS disorder_data_recency

FROM combined_data

ORDER BY person_id