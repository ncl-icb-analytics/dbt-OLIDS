{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All NHS Health Check completed events for all persons.
Uses a hardcoded list of SNOMED codes to identify completed health checks.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display
        
    FROM ({{ get_observations("'NHSHEALTHCHECK_COD'") }}) obs
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
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    
    -- All records represent completed NHS Health Checks
    TRUE AS is_completed_health_check,
    
    -- Calculate time since health check
    DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) AS days_since_health_check,
    
    -- Health check currency flags (standard intervals)
    CASE 
        WHEN DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS health_check_current_12m,
    
    CASE 
        WHEN DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) <= 730 THEN TRUE
        ELSE FALSE
    END AS health_check_current_24m,
    
    -- NHS Health Check cycle (5 years)
    CASE 
        WHEN DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) <= 1825 THEN TRUE
        ELSE FALSE
    END AS health_check_current_5y,
    
    -- Years since health check
    ROUND(DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) / 365.25, 1) AS years_since_health_check

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC 