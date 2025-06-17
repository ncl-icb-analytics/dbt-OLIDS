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
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag osteoporosis diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'OSTEO_COD' THEN TRUE ELSE FALSE END AS is_osteoporosis_diagnosis_code
        
    FROM ({{ get_observations("'OSTEO_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_osteoporosis_diagnosis_code

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC 