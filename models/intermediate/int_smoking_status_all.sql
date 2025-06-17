{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All smoking status observations from clinical records.
Uses QOF definitions with cluster IDs: SMOK_COD (general), LSMOK_COD (current smoker), 
EXSMOK_COD (ex-smoker), and NSMOK_COD (never smoked) codes.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag different types of smoking codes
        CASE WHEN obs.source_cluster_id = 'LSMOK_COD' THEN TRUE ELSE FALSE END AS is_smoker_code,
        CASE WHEN obs.source_cluster_id = 'EXSMOK_COD' THEN TRUE ELSE FALSE END AS is_ex_smoker_code,
        CASE WHEN obs.source_cluster_id = 'NSMOK_COD' THEN TRUE ELSE FALSE END AS is_never_smoked_code
        
    FROM ({{ get_observations("'SMOK_COD', 'LSMOK_COD', 'EXSMOK_COD', 'NSMOK_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_smoker_code,
    is_ex_smoker_code,
    is_never_smoked_code,
    
    -- Derive smoking status based on the code type
    CASE 
        WHEN is_smoker_code THEN 'Current Smoker'
        WHEN is_ex_smoker_code THEN 'Ex-Smoker'
        WHEN is_never_smoked_code THEN 'Never Smoked'
        ELSE 'Unknown'
    END AS smoking_status,
    
    -- General smoking indicator
    CASE 
        WHEN is_smoker_code THEN TRUE
        ELSE FALSE
    END AS is_current_smoker,
    
    -- Ex-smoker indicator
    CASE 
        WHEN is_ex_smoker_code THEN TRUE
        ELSE FALSE
    END AS is_ex_smoker

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC 