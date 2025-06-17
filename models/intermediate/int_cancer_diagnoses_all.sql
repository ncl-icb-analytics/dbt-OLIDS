{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All cancer diagnoses from clinical records.
Uses QOF cluster ID CAN_COD for cancer diagnosis codes (excluding non-melanotic skin cancers).

Clinical Purpose:
- Cancer register inclusion for QOF quality measures
- Cancer care pathway tracking
- Survivorship care planning

QOF Context:
Cancer register follows simple diagnosis-only pattern with date restriction - any cancer 
diagnosis on/after April 1, 2003 qualifies for register inclusion. No resolution codes.
This supports cancer care quality measures and survivorship monitoring.

Note: Excludes non-melanotic skin cancers as per QOF guidelines.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for cancer register.
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value,
        
        -- Cancer-specific flags
        CASE WHEN obs.cluster_id = 'CAN_COD' THEN TRUE ELSE FALSE END AS is_cancer_diagnosis
        
    FROM ({{ get_observations("'CAN_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    SELECT
        person_id,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_cancer_date,
        MAX(clinical_effective_date) AS latest_cancer_date,
        COUNT(DISTINCT clinical_effective_date) AS total_cancer_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_cancer_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_cancer_concept_displays
        
    FROM base_observations
    GROUP BY person_id
)

SELECT 
    bo.person_id,
    bo.observation_id,
    bo.clinical_effective_date,
    bo.concept_code,
    bo.concept_display,
    bo.source_cluster_id,
    
    -- Cancer-specific flags
    bo.is_cancer_diagnosis,
    
    -- Person-level aggregate context
    pa.earliest_cancer_date,
    pa.latest_cancer_date,
    pa.total_cancer_episodes,
    pa.all_cancer_concept_codes,
    pa.all_cancer_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 