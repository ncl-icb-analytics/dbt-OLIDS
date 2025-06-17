{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All gestational diabetes diagnosis observations from clinical records.
Uses QOF gestational diabetes cluster ID:
- GESTDIAB_COD: Gestational diabetes diagnoses

Clinical Purpose:
- QOF gestational diabetes register data collection
- Pregnancy-related diabetes monitoring
- Postpartum diabetes risk assessment
- Future diabetes prevention planning

Key QOF Requirements:
- Register inclusion: Gestational diabetes diagnosis (GESTDIAB_COD)
- No resolution codes - gestational diabetes is condition-specific to pregnancy
- Usually applies to women of childbearing age
- Important for postpartum diabetes screening

Note: Gestational diabetes does not have resolved codes as it is specific to pregnancy episodes.
The register tracks diagnosis history which informs future diabetes risk.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_gestational_diabetes_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag gestational diabetes diagnosis codes following QOF definitions
        CASE WHEN obs.cluster_id = 'GESTDIAB_COD' THEN TRUE ELSE FALSE END AS is_gestational_diabetes_diagnosis_code
        
    FROM ({{ get_observations("'GESTDIAB_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    SELECT
        person_id,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_gestational_diabetes_date,
        MAX(clinical_effective_date) AS latest_gestational_diabetes_date,
        COUNT(DISTINCT clinical_effective_date) AS total_gestational_diabetes_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_gestational_diabetes_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_gestational_diabetes_concept_displays
        
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
    
    -- Gestational diabetes-specific flags
    bo.is_gestational_diabetes_diagnosis_code,
    
    -- Person-level aggregate context
    pa.earliest_gestational_diabetes_date,
    pa.latest_gestational_diabetes_date,
    pa.total_gestational_diabetes_episodes,
    pa.all_gestational_diabetes_concept_codes,
    pa.all_gestational_diabetes_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 