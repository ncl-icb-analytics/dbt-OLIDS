{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Intermediate Ethnicity All - Complete ethnicity observations
-- Uses broader ethnicity mapping via ETHNICITY_CODES reference table`
-- Includes ALL persons regardless of active status

WITH ethnicity_source_concepts AS (
    -- First identify all source concept IDs that map to ethnicity codes
    SELECT DISTINCT
        cm.source_code_id,
        c.code AS concept_code,
        c.display AS concept_display,
        c.id AS concept_id
    FROM {{ ref('stg_reference_ethnicity_codes') }} AS ec
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} AS c
        ON ec.code = c.code
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} AS cm
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
        ec.granular AS ethnicity_granular,

        /* Normalised uppercase helpers */
        UPPER(TRIM(ec.term))        AS term_upper,
        UPPER(TRIM(ec.category))    AS cat_upper,
        UPPER(TRIM(ec.subcategory)) AS sub_upper,
        UPPER(TRIM(ec.granular))    AS gran_upper
    FROM ethnicity_observations AS eo
    -- Join to ethnicity codes to get the detailed categorisation
    LEFT JOIN {{ ref('stg_reference_ethnicity_codes') }} AS ec
        ON eo.concept_code = ec.code
),

flags_stage AS (
    /* Compute core flags first so they can be referenced later */
    SELECT
        eef.*,
        /* Unknown-like buckets */
        CASE
            WHEN eef.cat_upper = 'UNKNOWN'
              OR eef.sub_upper IN ('RECORDED NOT KNOWN','NOT RECORDED','NOT STATED','REFUSED')
            THEN TRUE ELSE FALSE
        END AS is_unknown_like,
        /* Explicitly unhelpful content by term (nationality phrasing etc.) */
        CASE
            WHEN eef.term_upper = 'BRITISH OR MIXED BRITISH' THEN TRUE
            ELSE FALSE
        END AS is_unhelpful_specific,
        /* Generic-other only when granular itself is non-specific */
        CASE
            WHEN eef.gran_upper IS NULL
              OR eef.gran_upper IN (
                    'OTHER','WHITE - OTHER','BLACK - OTHER','ASIAN - OTHER',
                    'OTHER MIXED','MIXED','OTHER ASIAN','ASIAN - OTHER')
            THEN TRUE ELSE FALSE
        END AS is_generic_other
    FROM ethnicity_enriched AS eef
),

ethnicity_enriched_with_flags AS (
    /* Compute deprioritisation flags and stable sort helpers using precomputed flags */
    SELECT
        fs.*,
        /* Preference: 1 best (specific), 2 generic-other, 3 unknown/refused or explicitly unhelpful */
        CASE
            WHEN (fs.is_unknown_like OR fs.is_unhelpful_specific) THEN 3
            WHEN fs.is_generic_other THEN 2
            ELSE 1
        END AS preference_rank,
        /* Category ordering for charts: alphabetical with White before Other, Unknown last */
        CASE fs.cat_upper
            WHEN 'ASIAN' THEN 1
            WHEN 'BLACK' THEN 2
            WHEN 'MIXED' THEN 3
            WHEN 'WHITE' THEN 4
            WHEN 'OTHER' THEN 5
            WHEN 'UNKNOWN' THEN 6
            ELSE 7
        END AS category_sort,
        /* Subcategory label: text after "Category: " when present */
        TRIM(COALESCE(REGEXP_SUBSTR(fs.ethnicity_subcategory, '^[^:]+:\s*(.*)$', 1, 1, 'i', 1), fs.ethnicity_subcategory)) AS subcategory_label,
        /* Bucket within category: specific first, then Other*, then Unknown/Refused */
        CASE
            WHEN fs.is_unknown_like THEN '99'
            WHEN (fs.sub_upper LIKE '%OTHER%' OR fs.gran_upper IN ('OTHER','WHITE - OTHER','BLACK - OTHER','ASIAN - OTHER','OTHER MIXED','MIXED','OTHER ASIAN','ASIAN - OTHER')) THEN '90'
            ELSE '00'
        END AS subcategory_bucket,
        /* British spelling */
        (fs.is_unknown_like OR fs.is_unhelpful_specific OR fs.is_generic_other) AS deprioritise_flag,
        /* Stable sort key without SNOMED code */
        LPAD(category_sort::STRING, 2, '0') || '_' ||
        subcategory_bucket || '_' ||
        UPPER(COALESCE(subcategory_label, '')) || '_' ||
        UPPER(COALESCE(fs.ethnicity_granular, fs.term, '')) AS display_sort_key
    FROM flags_stage AS fs
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
    ,
    deprioritise_flag,
    preference_rank,
    category_sort,
    display_sort_key
FROM ethnicity_enriched_with_flags
ORDER BY person_id ASC, clinical_effective_date DESC
