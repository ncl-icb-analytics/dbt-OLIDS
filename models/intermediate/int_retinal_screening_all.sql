{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All diabetes retinal screening programme completions with enhanced analytics features.
Uses RETSCREN_COD cluster which only includes completed screenings 
(excludes declined, unsuitable, or referral codes).

Enhanced Analytics Features:
- Legacy structure alignment with sk_patient_id
- Comprehensive screening result categorisation
- Diabetes eye risk assessment and interpretation
- Enhanced clinical context and timeframe analysis
- QOF diabetes care process integration support

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    ap.sk_patient_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    
    -- All records in this model represent completed screenings
    TRUE AS is_completed_screening,
    TRUE AS is_retinal_screening_code,
    
    -- Enhanced screening result categorisation
    CASE 
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' OR LOWER(obs.mapped_concept_display) LIKE '%no abnormality%' THEN 'Normal'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%background%' OR LOWER(obs.mapped_concept_display) LIKE '%mild%' THEN 'Background Retinopathy'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%pre-proliferative%' OR LOWER(obs.mapped_concept_display) LIKE '%moderate%' THEN 'Pre-proliferative'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%proliferative%' OR LOWER(obs.mapped_concept_display) LIKE '%severe%' THEN 'Proliferative'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%maculopathy%' OR LOWER(obs.mapped_concept_display) LIKE '%macular%' THEN 'Maculopathy'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%referral%' OR LOWER(obs.mapped_concept_display) LIKE '%refer%' THEN 'Referral Required'
        ELSE 'Screening Completed'
    END AS screening_result_category,
    
    -- Diabetes eye risk assessment
    CASE 
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' OR LOWER(obs.mapped_concept_display) LIKE '%no abnormality%' 
        THEN 'Low Risk (Normal)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%background%' OR LOWER(obs.mapped_concept_display) LIKE '%mild%' 
        THEN 'Low-Moderate Risk (Background)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%pre-proliferative%' OR LOWER(obs.mapped_concept_display) LIKE '%moderate%' 
        THEN 'Moderate Risk (Pre-proliferative)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%proliferative%' OR LOWER(obs.mapped_concept_display) LIKE '%severe%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%maculopathy%' OR LOWER(obs.mapped_concept_display) LIKE '%macular%'
        THEN 'High Risk (Sight-threatening)'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%referral%' OR LOWER(obs.mapped_concept_display) LIKE '%refer%' 
        THEN 'Requires Clinical Assessment'
        ELSE 'Risk Assessment Required'
    END AS diabetes_eye_risk_category,
    
    -- Clinical flags for analytics
    CASE 
        WHEN LOWER(obs.mapped_concept_display) LIKE '%normal%' OR LOWER(obs.mapped_concept_display) LIKE '%no abnormality%' 
        THEN TRUE ELSE FALSE 
    END AS is_normal_screening,
    
    CASE 
        WHEN LOWER(obs.mapped_concept_display) LIKE '%proliferative%' OR LOWER(obs.mapped_concept_display) LIKE '%severe%' 
            OR LOWER(obs.mapped_concept_display) LIKE '%maculopathy%' OR LOWER(obs.mapped_concept_display) LIKE '%macular%'
        THEN TRUE ELSE FALSE 
    END AS is_sight_threatening_retinopathy,
    
    CASE 
        WHEN LOWER(obs.mapped_concept_display) LIKE '%referral%' OR LOWER(obs.mapped_concept_display) LIKE '%refer%' 
        THEN TRUE ELSE FALSE 
    END AS requires_ophthalmology_referral,
    
    -- Enhanced time calculations
    DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) AS days_since_screening,
    ROUND(DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) / 365.25, 1) AS years_since_screening,
    
    -- Screening currency flags (diabetes care process requirements)
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS screening_current_12m,
    
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 730 THEN TRUE
        ELSE FALSE
    END AS screening_current_24m,
    
    -- QOF diabetes care process flag
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS meets_qof_screening_requirement,
    
    -- Clinical interpretation for reporting
    CASE 
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 
        THEN 'Current (within 12 months)'
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 730 
        THEN 'Recent (within 24 months)'
        ELSE 'Overdue (>24 months)'
    END AS screening_status_interpretation
        
FROM ({{ get_observations("'RETSCREN_COD'") }}) obs
LEFT JOIN {{ ref('dim_person_active_patients') }} ap
    ON obs.person_id = ap.person_id
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC 