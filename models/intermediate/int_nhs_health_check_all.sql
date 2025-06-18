{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All NHS Health Check completed events with enhanced analytics features.
Uses NHSHEALTHCHECK_COD cluster with validated SNOMED codes for completed health checks.

Enhanced Analytics Features:
- Legacy structure alignment with sk_patient_id
- Health check type classification and eligibility assessment
- Enhanced timeframe analysis and currency tracking
- QOF prevention pathway integration support
- Risk factor assessment context

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    ap.sk_patient_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    'NHSHEALTHCHECK_COD' AS source_cluster_id,
    
    -- All records represent completed NHS Health Checks
    TRUE AS is_completed_health_check,
    TRUE AS is_nhs_health_check_code,
    
    -- Health check type classification based on concept codes
    CASE 
        WHEN obs.mapped_concept_code IN ('1959151000006103', '523221000000100') THEN 'Standard NHS Health Check'
        WHEN obs.mapped_concept_code IN ('1948791000006100', '1728781000006106') THEN 'Initial NHS Health Check'
        WHEN obs.mapped_concept_code IN ('1728811000006108', '1728801000006105', '1728791000006109') THEN 'Follow-up NHS Health Check'
        WHEN obs.mapped_concept_code IN ('840391000000101', '840401000000103') THEN 'Cardiovascular Risk Assessment'
        WHEN obs.mapped_concept_code IN ('1053551000000105', '904471000000104', '904481000000102') THEN 'Health Check Review'
        ELSE 'NHS Health Check'
    END AS health_check_type_classification,
    
    -- Clinical context for health checks
    CASE 
        WHEN obs.mapped_concept_code IN ('840391000000101', '840401000000103') THEN 'CVD Risk Assessment Focus'
        WHEN obs.mapped_concept_code IN ('1053551000000105', '904471000000104') THEN 'Review and Follow-up'
        ELSE 'Prevention and Early Detection'
    END AS health_check_clinical_context,
    
    -- Age-based eligibility context (using age dimension)
    CASE 
        WHEN age.age BETWEEN 40 AND 74 THEN 'Eligible Age Group (40-74)'
        WHEN age.age < 40 THEN 'Below Standard Age (Under 40)'
        WHEN age.age > 74 THEN 'Above Standard Age (Over 74)'
        ELSE 'Age Assessment Required'
    END AS eligibility_status_by_age,
    
    -- Clinical flags for analytics
    CASE 
        WHEN obs.mapped_concept_code IN ('840391000000101', '840401000000103') THEN TRUE 
        ELSE FALSE 
    END AS includes_cvd_risk_assessment,
    
    CASE 
        WHEN obs.mapped_concept_code IN ('1728811000006108', '1728801000006105', '1728791000006109', 
                                         '1053551000000105', '904471000000104', '904481000000102') THEN TRUE 
        ELSE FALSE 
    END AS is_follow_up_check,
    
    -- Enhanced time calculations
    DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) AS days_since_health_check,
    ROUND(DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) / 365.25, 1) AS years_since_health_check,
    
    -- Health check currency flags (standard intervals)
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS health_check_current_12m,
    
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 730 THEN TRUE
        ELSE FALSE
    END AS health_check_current_24m,
    
    -- NHS Health Check cycle (5 years)
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 1825 THEN TRUE
        ELSE FALSE
    END AS health_check_current_5y,
    
    -- QOF prevention pathway flags
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 1825 THEN TRUE
        ELSE FALSE
    END AS meets_qof_prevention_requirement,
    
    -- Clinical interpretation for reporting
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 1825 
        THEN 'Current (within 5 years)'
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 2555 
        THEN 'Recent (within 7 years)'
        ELSE 'Overdue (>7 years)'
    END AS health_check_status_interpretation
        
FROM ({{ get_observations("'NHSHEALTHCHECK_COD'") }}) obs
LEFT JOIN {{ ref('dim_person_active_patients') }} ap
    ON obs.person_id = ap.person_id
LEFT JOIN {{ ref('dim_person_age') }} age
    ON obs.person_id = age.person_id
WHERE obs.clinical_effective_date IS NOT NULL
  AND obs.mapped_concept_code IN (
      '1959151000006103',
      '1948791000006100',
      '1728781000006106',
      '523221000000100',
      '1728811000006108',
      '1728801000006105',
      '1728791000006109',
      '840391000000101',
      '840401000000103',
      '1053551000000105',
      '904471000000104',
      '904481000000102'
  )

ORDER BY person_id, clinical_effective_date DESC 