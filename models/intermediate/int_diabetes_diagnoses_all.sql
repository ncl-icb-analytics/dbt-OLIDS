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
Use this model as input for fct_person_diabetes_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
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
        CASE WHEN obs.cluster_id = 'DMRES_COD' THEN TRUE ELSE FALSE END AS is_diabetes_resolved_code
        
    FROM ({{ get_observations("'DM_COD', 'DMTYPE1_COD', 'DMTYPE2_COD', 'DMRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level diabetes date aggregates for context
    SELECT
        person_id,
        
        -- General diabetes dates
        MIN(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS earliest_diabetes_date,
        MAX(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS latest_diabetes_date,
        
        -- Type-specific dates
        MIN(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END) AS earliest_type1_date,
        MAX(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END) AS latest_type1_date,
        MIN(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END) AS earliest_type2_date,
        MAX(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END) AS latest_type2_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Concept code arrays for traceability (using conditional aggregation for Snowflake compatibility)
        ARRAY_AGG(DISTINCT CASE WHEN is_general_diabetes_code THEN concept_code ELSE NULL END) AS all_diabetes_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_general_diabetes_code THEN concept_display ELSE NULL END) AS all_diabetes_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_type1_diabetes_code THEN concept_code ELSE NULL END) AS all_type1_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_type2_diabetes_code THEN concept_code ELSE NULL END) AS all_type2_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_diabetes_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes
            
    FROM base_observations
    GROUP BY person_id
)

SELECT 
    bo.person_id,
    bo.observation_id,
    bo.clinical_effective_date,
    bo.mapped_concept_code AS concept_code,
    bo.mapped_concept_display AS concept_display,
    bo.cluster_id AS source_cluster_id,
    
    -- Diabetes type flags
    bo.is_general_diabetes_code,
    bo.is_type1_diabetes_code,
    bo.is_type2_diabetes_code,
    bo.is_diabetes_resolved_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_diabetes_date,
    pa.latest_diabetes_date,
    pa.earliest_type1_date,
    pa.latest_type1_date,
    pa.earliest_type2_date,
    pa.latest_type2_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_diabetes_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_diabetes_currently_resolved,
    
    -- Diabetes type determination (for individual observation context)
    CASE
        WHEN bo.is_type1_diabetes_code THEN 'Type 1'
        WHEN bo.is_type2_diabetes_code THEN 'Type 2' 
        WHEN bo.is_general_diabetes_code THEN 'General'
        WHEN bo.is_diabetes_resolved_code THEN 'Resolved'
        ELSE 'Unknown'
    END AS diabetes_observation_type,
    
    -- Traceability arrays
    pa.all_diabetes_concept_codes,
    pa.all_diabetes_concept_displays,
    pa.all_type1_concept_codes,
    pa.all_type2_concept_codes,
    pa.all_resolved_concept_codes

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 