{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Intermediate Ethnicity All - Complete ethnicity observations
-- Uses broader ethnicity mapping via ETHNICITY_CODES reference table
-- Includes ALL persons regardless of active status

WITH observations_with_concepts AS (
    -- Join observations directly through concept_map to concept, using person_id directly from observations
    SELECT
        o.id AS observation_id,
        o.patient_id,
        o.person_id,
        NULL AS sk_patient_id,  -- Will be populated later if needed
        o.clinical_effective_date,
        c.code AS concept_code,
        c.display AS concept_display,
        c.id AS concept_id
    FROM {{ ref('stg_olids_observation') }} AS o
    -- Join through concept_map to concept (vanilla structure)
    LEFT JOIN {{ ref('stg_olids_term_concept_map') }} AS cm
        ON o.observation_source_concept_id = cm.source_code_id
    LEFT JOIN {{ ref('stg_olids_term_concept') }} AS c
        ON cm.target_code_id = c.id
    WHERE
        o.clinical_effective_date IS NOT NULL
        AND c.code IS NOT NULL
        AND o.person_id IS NOT NULL
),

ethnicity_observations AS (
    -- Filter observations that match ethnicity codes
    SELECT
        owc.person_id,
        owc.sk_patient_id,
        owc.clinical_effective_date,
        owc.concept_id,
        owc.concept_code,
        owc.concept_display,
        owc.observation_id
    FROM observations_with_concepts AS owc
    -- Join to ethnicity codes to filter only valid ethnicity observations
    INNER JOIN {{ ref('stg_codesets_ethnicity_codes') }} AS ec
        ON owc.concept_code = ec.code
),

ethnicity_enriched AS (
    -- Add ethnicity categorisation details from ethnicity codes reference table
    SELECT
        eo.*,
        ec.term,
        ec.category AS ethnicity_category,
        ec.subcategory AS ethnicity_subcategory,
        ec.granular AS ethnicity_granular
    FROM ethnicity_observations AS eo
    -- Join to ethnicity codes to get the detailed categorisation
    LEFT JOIN {{ ref('stg_codesets_ethnicity_codes') }} AS ec
        ON eo.concept_code = ec.code
)

-- Final selection with enriched ethnicity data
SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date,
    concept_id,
    concept_code AS snomed_code,
    observation_id AS observation_lds_id,
    COALESCE(term, concept_display) AS term,
    COALESCE(ethnicity_category, 'Unknown') AS ethnicity_category,
    COALESCE(ethnicity_subcategory, 'Unknown') AS ethnicity_subcategory,
    COALESCE(ethnicity_granular, 'Unknown') AS ethnicity_granular
FROM ethnicity_enriched
ORDER BY person_id ASC, clinical_effective_date DESC
