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
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of NDH codes following QOF definitions
        CASE WHEN obs.cluster_id = 'NDH_COD' THEN TRUE ELSE FALSE END AS is_ndh_diagnosis_code,
        CASE WHEN obs.cluster_id = 'IGT_COD' THEN TRUE ELSE FALSE END AS is_igt_diagnosis_code,
        CASE WHEN obs.cluster_id = 'PRD_COD' THEN TRUE ELSE FALSE END AS is_pre_diabetes_diagnosis_code
        
    FROM ({{ get_observations("'NDH_COD', 'IGT_COD', 'PRD_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level NDH date aggregates for context
    SELECT
        person_id,
        
        -- NDH diagnosis dates
        MIN(CASE WHEN is_ndh_diagnosis_code THEN clinical_effective_date END) AS earliest_ndh_date,
        MAX(CASE WHEN is_ndh_diagnosis_code THEN clinical_effective_date END) AS latest_ndh_date,
        
        -- IGT diagnosis dates
        MIN(CASE WHEN is_igt_diagnosis_code THEN clinical_effective_date END) AS earliest_igt_date,
        MAX(CASE WHEN is_igt_diagnosis_code THEN clinical_effective_date END) AS latest_igt_date,
        
        -- Pre-diabetes dates
        MIN(CASE WHEN is_pre_diabetes_diagnosis_code THEN clinical_effective_date END) AS earliest_prd_date,
        MAX(CASE WHEN is_pre_diabetes_diagnosis_code THEN clinical_effective_date END) AS latest_prd_date,
        
        -- Multi-type NDH dates (any NDH/IGT/PRD)
        MIN(clinical_effective_date) AS earliest_multndh_date,
        MAX(clinical_effective_date) AS latest_multndh_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_ndh_diagnosis_code THEN concept_code ELSE NULL END) AS all_ndh_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_ndh_diagnosis_code THEN concept_display ELSE NULL END) AS all_ndh_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_igt_diagnosis_code THEN concept_code ELSE NULL END) AS all_igt_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_igt_diagnosis_code THEN concept_display ELSE NULL END) AS all_igt_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_pre_diabetes_diagnosis_code THEN concept_code ELSE NULL END) AS all_prd_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_pre_diabetes_diagnosis_code THEN concept_display ELSE NULL END) AS all_prd_concept_displays
            
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
    
    -- NDH type flags
    bo.is_ndh_diagnosis_code,
    bo.is_igt_diagnosis_code,
    bo.is_pre_diabetes_diagnosis_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_ndh_date,
    pa.latest_ndh_date,
    pa.earliest_igt_date,
    pa.latest_igt_date,
    pa.earliest_prd_date,
    pa.latest_prd_date,
    pa.earliest_multndh_date,
    pa.latest_multndh_date,
    
    -- Traceability arrays
    pa.all_ndh_concept_codes,
    pa.all_ndh_concept_displays,
    pa.all_igt_concept_codes,
    pa.all_igt_concept_displays,
    pa.all_prd_concept_codes,
    pa.all_prd_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date DESC 