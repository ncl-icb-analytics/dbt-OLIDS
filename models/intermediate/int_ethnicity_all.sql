{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate table containing all dated Observation records that map to a valid code in the ETHNICITY_CODES reference table. Used as a source for DIM_PERSON_ETHNICITY to find the latest valid record per person.'"
        ]
    )
}}

-- Intermediate Ethnicity All - Complete ethnicity observations
-- Uses broader ethnicity mapping via ETHNICITY_CODES reference table
-- Includes ALL persons regardless of active status

WITH mapped_observations AS (
    -- Get all observations with proper concept mapping
    SELECT 
        o.id AS observation_id,
        o.patient_id,
        pp.person_id,
        p.sk_patient_id,
        o.clinical_effective_date,
        o.observation_core_concept_id,
        mc.concept_id AS mapped_concept_id,
        mc.concept_code AS mapped_concept_code,
        mc.code_description AS mapped_concept_display
    FROM {{ ref('stg_olids_observation') }} o
    JOIN {{ ref('stg_olids_patient') }} p
        ON o.patient_id = p.id
    JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON p.id = pp.patient_id
    JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
        ON o.observation_core_concept_id = mc.source_code_id
    WHERE o.clinical_effective_date IS NOT NULL
),

ethnicity_observations AS (
    -- Filter observations that match ethnicity codes
    SELECT 
        mo.person_id,
        mo.sk_patient_id,
        mo.clinical_effective_date,
        mo.mapped_concept_id,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        mo.observation_id
    FROM mapped_observations mo
    -- Join to ethnicity codes to filter only valid ethnicity observations
    INNER JOIN {{ ref('stg_codesets_ethnicity_codes') }} ec
        ON mo.mapped_concept_code = ec.code
),

ethnicity_enriched AS (
    -- Add ethnicity categorisation details from ethnicity codes reference table
    SELECT 
        eo.*,
        ec.term,
        ec.category AS ethnicity_category,
        ec.subcategory AS ethnicity_subcategory,
        ec.granular AS ethnicity_granular
    FROM ethnicity_observations eo
    -- Join to ethnicity codes to get the detailed categorisation
    LEFT JOIN {{ ref('stg_codesets_ethnicity_codes') }} ec
        ON eo.mapped_concept_code = ec.code
)

-- Final selection with enriched ethnicity data
SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date,
    mapped_concept_id AS concept_id,
    mapped_concept_code AS snomed_code,
    COALESCE(term, mapped_concept_display) AS term,
    COALESCE(ethnicity_category, 'Unknown') AS ethnicity_category,
    COALESCE(ethnicity_subcategory, 'Unknown') AS ethnicity_subcategory,
    COALESCE(ethnicity_granular, 'Unknown') AS ethnicity_granular,
    observation_id AS observation_lds_id
FROM ethnicity_enriched
ORDER BY person_id, clinical_effective_date DESC 