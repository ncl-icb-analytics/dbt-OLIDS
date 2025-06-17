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
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag gestational diabetes diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'GESTDIAB_COD' THEN TRUE ELSE FALSE END AS is_gestational_diabetes_diagnosis_code
        
    FROM ({{ get_observations("'GESTDIAB_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_gestational_diabetes_diagnosis_code

FROM base_observations

-- Sort for consistent output
ORDER BY obs.person_id, obs.clinical_effective_date DESC 