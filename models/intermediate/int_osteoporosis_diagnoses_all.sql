{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All osteoporosis diagnosis observations from clinical records.
Uses QOF osteoporosis cluster ID:
- OSTEO_COD: Osteoporosis diagnoses

Clinical Purpose:
- QOF osteoporosis register data collection
- Bone health assessment
- Osteoporosis diagnosis tracking

Key QOF Requirements:
- Register inclusion: Osteoporosis diagnosis (OSTEO_COD) OR DXA confirmation
- DXA confirmation handled via separate int_dxa_scans_all model
- Combined logic applied in fact layer

Note: DXA scans and T-scores are handled in separate int_dxa_scans_all model.
The register logic combines clinical diagnosis with DXA confirmation in the fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_osteoporosis_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag osteoporosis diagnosis codes following QOF definitions
        CASE WHEN obs.cluster_id = 'OSTEO_COD' THEN TRUE ELSE FALSE END AS is_osteoporosis_diagnosis_code
        
    FROM ({{ get_observations("'OSTEO_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level osteoporosis date aggregates for context
    SELECT
        person_id,
        
        -- Osteoporosis diagnosis dates
        MIN(CASE WHEN is_osteoporosis_diagnosis_code THEN clinical_effective_date END) AS earliest_osteoporosis_date,
        MAX(CASE WHEN is_osteoporosis_diagnosis_code THEN clinical_effective_date END) AS latest_osteoporosis_date,
        
        -- Diagnosis flags for register logic
        CASE WHEN COUNT(CASE WHEN is_osteoporosis_diagnosis_code THEN 1 END) > 0 THEN TRUE ELSE FALSE END AS is_osteoporosis_diagnosis,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_osteoporosis_diagnosis_code THEN concept_code ELSE NULL END) AS all_osteoporosis_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_osteoporosis_diagnosis_code THEN concept_display ELSE NULL END) AS all_osteoporosis_concept_displays
            
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
    
    -- Osteoporosis diagnosis flags
    bo.is_osteoporosis_diagnosis_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_osteoporosis_date,
    pa.latest_osteoporosis_date,
    pa.is_osteoporosis_diagnosis,
    
    -- Traceability arrays
    pa.all_osteoporosis_concept_codes,
    pa.all_osteoporosis_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date DESC 