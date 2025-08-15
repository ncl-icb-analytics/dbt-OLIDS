{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All diabetes diagnosis observations from clinical records.
Uses QOF diabetes cluster IDs with clinical prioritization:
- DMTYPE1_COD: Type 1 diabetes specific diagnoses (highest priority)
- DMTYPE2_COD: Type 2 diabetes specific diagnoses
- DM_COD: General diabetes diagnoses
- DMRES_COD: Diabetes resolved/remission codes (lowest priority)

Clinical Purpose:
- QOF diabetes register data collection
- Diabetes type classification support
- Disease progression tracking
- Resolution status monitoring

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per diabetes observation.
When an observation belongs to multiple clusters, we flag ALL applicable categories rather than just the most specific.
Use this model as input for fct_person_diabetes_register.sql which applies person-level aggregation and QOF business rules.
*/

WITH diabetes_observations_all_clusters AS (
    -- Get all diabetes observations with all their cluster relationships
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'DM_COD', 'DMTYPE1_COD', 'DMTYPE2_COD', 'DMRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

diabetes_observations_categorised AS (
    -- Flag ALL applicable categories per observation (not just highest priority)
    SELECT
        observation_id,
        person_id,
        clinical_effective_date,
        concept_code,
        concept_display,
        
        -- Flag all applicable diabetes categories for each observation
        MAX(CASE WHEN source_cluster_id = 'DM_COD' THEN TRUE ELSE FALSE END) AS is_general_diabetes_code,
        MAX(CASE WHEN source_cluster_id = 'DMTYPE1_COD' THEN TRUE ELSE FALSE END) AS is_type1_diabetes_code,
        MAX(CASE WHEN source_cluster_id = 'DMTYPE2_COD' THEN TRUE ELSE FALSE END) AS is_type2_diabetes_code,
        MAX(CASE WHEN source_cluster_id = 'DMRES_COD' THEN TRUE ELSE FALSE END) AS is_diabetes_resolved_code,
        
        -- For traceability, keep the most specific cluster as source_cluster_id
        CASE 
            WHEN MAX(CASE WHEN source_cluster_id = 'DMTYPE1_COD' THEN 1 ELSE 0 END) = 1 THEN 'DMTYPE1_COD'
            WHEN MAX(CASE WHEN source_cluster_id = 'DMTYPE2_COD' THEN 1 ELSE 0 END) = 1 THEN 'DMTYPE2_COD'
            WHEN MAX(CASE WHEN source_cluster_id = 'DM_COD' THEN 1 ELSE 0 END) = 1 THEN 'DM_COD'
            WHEN MAX(CASE WHEN source_cluster_id = 'DMRES_COD' THEN 1 ELSE 0 END) = 1 THEN 'DMRES_COD'
            ELSE 'UNKNOWN'
        END AS source_cluster_id
    FROM diabetes_observations_all_clusters
    GROUP BY observation_id, person_id, clinical_effective_date, concept_code, concept_display
)

SELECT
    observation_id,
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,

    -- Flag different types of diabetes codes following QOF definitions
    -- Now using the cumulative flags from the categorised CTE
    is_general_diabetes_code,
    is_type1_diabetes_code,
    is_type2_diabetes_code,
    is_diabetes_resolved_code,

    -- Diabetes type determination (for individual observation context)
    CASE
        WHEN source_cluster_id = 'DMTYPE1_COD' THEN 'Type 1'
        WHEN source_cluster_id = 'DMTYPE2_COD' THEN 'Type 2'
        WHEN source_cluster_id = 'DM_COD' THEN 'General'
        WHEN source_cluster_id = 'DMRES_COD' THEN 'Resolved'
        ELSE 'Unknown'
    END AS diabetes_observation_type

FROM diabetes_observations_categorised
ORDER BY person_id, clinical_effective_date, observation_id
