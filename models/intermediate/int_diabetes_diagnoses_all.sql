{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All diabetes diagnosis observations from clinical records.
Uses QOF diabetes cluster IDs:
- DM_COD: General diabetes diagnoses
- DMTYPE1_COD: Type 1 diabetes specific diagnoses  
- DMTYPE2_COD: Type 2 diabetes specific diagnoses
- DMRES_COD: Diabetes resolved/remission codes

Clinical Purpose:
- QOF diabetes register data collection
- Diabetes type classification support
- Disease progression tracking
- Resolution status monitoring

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per diabetes observation.
Use this model as input for fct_person_diabetes_register.sql which applies person-level aggregation and QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    
    -- Flag different types of diabetes codes following QOF definitions
    CASE WHEN obs.cluster_id = 'DM_COD' THEN TRUE ELSE FALSE END AS is_general_diabetes_code,
    CASE WHEN obs.cluster_id = 'DMTYPE1_COD' THEN TRUE ELSE FALSE END AS is_type1_diabetes_code,
    CASE WHEN obs.cluster_id = 'DMTYPE2_COD' THEN TRUE ELSE FALSE END AS is_type2_diabetes_code,
    CASE WHEN obs.cluster_id = 'DMRES_COD' THEN TRUE ELSE FALSE END AS is_diabetes_resolved_code,
    
    -- Diabetes type determination (for individual observation context)
    CASE
        WHEN obs.cluster_id = 'DMTYPE1_COD' THEN 'Type 1'
        WHEN obs.cluster_id = 'DMTYPE2_COD' THEN 'Type 2' 
        WHEN obs.cluster_id = 'DM_COD' THEN 'General'
        WHEN obs.cluster_id = 'DMRES_COD' THEN 'Resolved'
        ELSE 'Unknown'
    END AS diabetes_observation_type

FROM ({{ get_observations("'DM_COD', 'DMTYPE1_COD', 'DMTYPE2_COD', 'DMRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date, observation_id 