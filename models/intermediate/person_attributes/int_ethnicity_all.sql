{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Intermediate Ethnicity All - Complete ethnicity observations
-- Uses broader ethnicity mapping via ETHNICITY_CODES reference table
-- Includes ALL persons regardless of active status

WITH ethnicity_source_concepts AS (
    -- First identify all source concept IDs that map to ethnicity codes
    SELECT DISTINCT
        cm.source_code_id,
        c.code AS concept_code,
        c.display AS concept_display,
        c.id AS concept_id
    FROM {{ ref('stg_codesets_ethnicity_codes') }} AS ec
    INNER JOIN {{ ref('stg_olids_term_concept') }} AS c
        ON ec.code = c.code
    INNER JOIN {{ ref('stg_olids_term_concept_map') }} AS cm
        ON c.id = cm.target_code_id
),

ethnicity_observations AS (
    -- Now get only observations that have ethnicity-related source concepts
    SELECT
        o.id AS observation_id,
        o.patient_id,
        pp.person_id,
        p.sk_patient_id,
        o.clinical_effective_date,
        esc.concept_id,
        esc.concept_code,
        esc.concept_display
    FROM {{ ref('stg_olids_observation') }} AS o
    -- Filter to only ethnicity observations
    INNER JOIN ethnicity_source_concepts AS esc
        ON o.observation_source_concept_id = esc.source_code_id
    -- Join to patient to get sk_patient_id
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON o.patient_id = p.id
    -- Join to patient_person to get proper person_id
    INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
        ON p.id = pp.patient_id
    WHERE
        o.clinical_effective_date IS NOT NULL
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
