{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All non-diabetic hyperglycaemia (NDH) diagnosis observations from clinical records.
Uses QOF NDH cluster IDs:
- NDH_COD: Non-diabetic hyperglycaemia diagnoses
- IGT_COD: Impaired glucose tolerance diagnoses  
- PRD_COD: Pre-diabetes diagnoses

Clinical Purpose:
- QOF NDH register data collection (aged 18+, never had diabetes OR diabetes resolved)
- Pre-diabetes monitoring and intervention
- Diabetes prevention pathway support
- Glucose metabolism disorder tracking

Key QOF Requirements:
- Register inclusion: NDH/IGT/PRD diagnosis for patients aged 18+
- Exclusion: Current unresolved diabetes (handled in fact layer)
- Diabetes history integration required for eligibility
- Important for diabetes prevention programmes

Complex register requiring integration with diabetes diagnosis history.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_ndh_register.sql which applies QOF business rules and diabetes exclusions.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag different types of NDH codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'NDH_COD' THEN TRUE ELSE FALSE END AS is_ndh_diagnosis_code,
        CASE WHEN obs.source_cluster_id = 'IGT_COD' THEN TRUE ELSE FALSE END AS is_igt_diagnosis_code,
        CASE WHEN obs.source_cluster_id = 'PRD_COD' THEN TRUE ELSE FALSE END AS is_pre_diabetes_diagnosis_code
        
    FROM ({{ get_observations("'NDH_COD', 'IGT_COD', 'PRD_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ndh_diagnosis_code,
    is_igt_diagnosis_code,
    is_pre_diabetes_diagnosis_code

FROM base_observations

-- Sort for consistent output
ORDER BY obs.person_id, obs.clinical_effective_date DESC 