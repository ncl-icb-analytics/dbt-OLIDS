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
Enhanced with analytics-ready flags and legacy structure alignment.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS code_description,
        obs.cluster_id AS source_cluster_id,

        -- Flag different types of smoking codes
        CASE WHEN obs.cluster_id = 'LSMOK_COD' THEN TRUE ELSE FALSE END AS is_smoker_code,
        CASE WHEN obs.cluster_id = 'EXSMOK_COD' THEN TRUE ELSE FALSE END AS is_ex_smoker_code,
        CASE WHEN obs.cluster_id = 'NSMOK_COD' THEN TRUE ELSE FALSE END AS is_never_smoked_code,
        CASE WHEN obs.cluster_id = 'SMOK_COD' THEN TRUE ELSE FALSE END AS is_general_smoking_code

    FROM ({{ get_observations("'SMOK_COD', 'LSMOK_COD', 'EXSMOK_COD', 'NSMOK_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,

    -- Core boolean flags (matching legacy pattern)
    is_smoker_code,
    is_ex_smoker_code,
    is_never_smoked_code,

    -- Enhanced analytics fields
    -- Derive smoking status based on the code type
    CASE
        WHEN is_smoker_code THEN 'Current Smoker'
        WHEN is_ex_smoker_code THEN 'Ex-Smoker'
        WHEN is_never_smoked_code THEN 'Never Smoked'
        WHEN is_general_smoking_code THEN 'General Smoking Code'
        ELSE 'Unknown'
    END AS smoking_status,

    -- Current smoker indicator
    CASE
        WHEN is_smoker_code THEN TRUE
        ELSE FALSE
    END AS is_current_smoker,

    -- Ex-smoker indicator
    CASE
        WHEN is_ex_smoker_code THEN TRUE
        ELSE FALSE
    END AS is_ex_smoker,

    -- Never smoker indicator
    CASE
        WHEN is_never_smoked_code THEN TRUE
        ELSE FALSE
    END AS is_never_smoker,

    -- Analytics-ready risk flags
    CASE
        WHEN is_smoker_code OR is_ex_smoker_code THEN TRUE
        ELSE FALSE
    END AS has_smoking_history,

    -- QOF-specific categorisation for analytics
    CASE
        WHEN is_smoker_code THEN 'Active Smoker (QOF Risk)'
        WHEN is_ex_smoker_code THEN 'Ex-Smoker (CVD Risk)'
        WHEN is_never_smoked_code THEN 'Never Smoked (Low Risk)'
        ELSE 'Unknown Risk'
    END AS smoking_risk_category

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC
