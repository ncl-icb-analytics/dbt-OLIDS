{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'ethnicity'],
        cluster_by=['person_id'])
}}

-- Person Ethnicity Dimension Table
-- Holds the latest ethnicity record for ALL persons
-- Starts from PATIENT_PERSON and LEFT JOINs the latest ethnicity record if available
-- Ethnicity fields display 'Not Recorded' for persons with no recorded ethnicity

WITH latest_ethnicity_per_person AS (
    -- Identifies the single most recent ethnicity record for each person from the intermediate table
    -- Uses ROW_NUMBER() partitioned by person_id, ordered by deprioritise_flag (asc), preference_rank (asc),
    -- then clinical_effective_date (desc) and observation_lds_id (desc as tie-breaker)
    SELECT
        pea.person_id,
        pea.sk_patient_id,
        pea.clinical_effective_date,
        pea.concept_id,
        pea.snomed_code,
        pea.term,
        pea.ethnicity_category,
        pea.ethnicity_subcategory,
        pea.ethnicity_granular,
        pea.deprioritise_flag,
        pea.preference_rank,
        pea.category_sort,
        pea.display_sort_key,
        pea.observation_lds_id -- Include for potential tie-breaking
    FROM {{ ref('int_ethnicity_all') }} AS pea
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pea.person_id
        -- Prefer non-deprioritised and more specific first, then latest
        ORDER BY pea.deprioritise_flag ASC, pea.preference_rank ASC,
                 pea.clinical_effective_date DESC, pea.observation_lds_id DESC
    ) = 1 -- Get only the latest record per person
),

-- Constructs the final dimension by selecting all persons from the ethnicity records,
-- then ensuring complete coverage with all persons from the person dimension.
-- If a person has no ethnicity record, ethnicity-specific fields are populated with 'Not Recorded'.

-- First get all persons with ethnicity records
persons_with_ethnicity AS (
    SELECT
        lepp.person_id,
        lepp.sk_patient_id,
        lepp.clinical_effective_date AS latest_ethnicity_date,
        lepp.concept_id,
        lepp.snomed_code,
        lepp.term,
        lepp.ethnicity_category,
        lepp.ethnicity_subcategory,
        lepp.ethnicity_granular,
        lepp.deprioritise_flag,
        lepp.preference_rank,
        lepp.category_sort,
        lepp.display_sort_key
    FROM latest_ethnicity_per_person AS lepp
),

-- Then get all persons to ensure complete coverage
all_persons AS (
    SELECT 
        person_id,
        sk_patient_ids[0] AS sk_patient_id  -- Get first sk_patient_id from array
    FROM {{ ref('dim_person') }}
)

SELECT
    ap.person_id,
    COALESCE(pwe.sk_patient_id, ap.sk_patient_id) AS sk_patient_id,
    pwe.latest_ethnicity_date,
    COALESCE(pwe.concept_id, 'Not Recorded') AS concept_id,
    COALESCE(pwe.snomed_code, 'Not Recorded') AS snomed_code,
    COALESCE(pwe.term, 'Not Recorded') AS term,
    COALESCE(pwe.ethnicity_category, 'Not Recorded') AS ethnicity_category,
    COALESCE(pwe.ethnicity_subcategory, 'Not Recorded') AS ethnicity_subcategory,
    COALESCE(pwe.ethnicity_granular, 'Not Recorded') AS ethnicity_granular,
    /* expose sorting helpers for downstream charts */
    pwe.deprioritise_flag,
    pwe.preference_rank,
    pwe.category_sort,
    pwe.display_sort_key
FROM all_persons AS ap
LEFT JOIN persons_with_ethnicity AS pwe
    ON ap.person_id = pwe.person_id
