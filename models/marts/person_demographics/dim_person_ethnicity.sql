{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'ethnicity'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Person Ethnicity Dimension - Latest recorded ethnicity for population analysis.

Population Scope:
• All persons regardless of ethnicity record availability
• Latest ethnicity record per person where available
• Defaults to \"Not Recorded\" for persons without ethnicity data

Key Features:
• Ethnicity category, subcategory, and granular classifications
• SNOMED coding for clinical interoperability
• Clinical effective date for data quality assessment'"
        ]
    )
}}

-- Person Ethnicity Dimension Table
-- Holds the latest ethnicity record for ALL persons
-- Starts from PATIENT_PERSON and LEFT JOINs the latest ethnicity record if available
-- Ethnicity fields display 'Not Recorded' for persons with no recorded ethnicity

WITH latest_ethnicity_per_person AS (
    -- Identifies the single most recent ethnicity record for each person from the intermediate table
    -- Uses ROW_NUMBER() partitioned by person_id, ordered by clinical_effective_date (desc) and observation_lds_id (desc as tie-breaker)
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
        pea.observation_lds_id -- Include for potential tie-breaking
    FROM {{ ref('int_ethnicity_all') }} AS pea
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pea.person_id
        -- Order by date first, then by observation ID as a tie-breaker
        ORDER BY pea.clinical_effective_date DESC, pea.observation_lds_id DESC
    ) = 1 -- Get only the latest record per person
)

-- Constructs the final dimension by selecting all persons from PATIENT_PERSON and PATIENT tables,
-- then LEFT JOINing their latest ethnicity information (if available) from the LatestEthnicityPerPerson CTE.
-- If a person has no ethnicity record, ethnicity-specific fields are populated with 'Not Recorded'.
SELECT
    pp.person_id,
    p.sk_patient_id, -- Get sk_patient_id from PATIENT table
    -- Ethnicity fields from the latest record, using COALESCE for NULLs
    lepp.clinical_effective_date AS latest_ethnicity_date, -- Date remains NULL if no record
    COALESCE(lepp.concept_id, 'Not Recorded') AS concept_id,
    COALESCE(lepp.snomed_code, 'Not Recorded') AS snomed_code,
    COALESCE(lepp.term, 'Not Recorded') AS term,
    COALESCE(lepp.ethnicity_category, 'Not Recorded') AS ethnicity_category,
    COALESCE(lepp.ethnicity_subcategory, 'Not Recorded')
        AS ethnicity_subcategory,
    COALESCE(lepp.ethnicity_granular, 'Not Recorded') AS ethnicity_granular
FROM {{ ref('stg_olids_patient_person') }} AS pp -- Start with all persons
-- Use LEFT JOIN to keep persons even if no PATIENT record (unlikely but safe)
LEFT JOIN {{ ref('stg_olids_patient') }} AS p
    ON pp.patient_id = p.id
-- Use LEFT JOIN to keep all persons, regardless of whether they have an ethnicity record
LEFT JOIN latest_ethnicity_per_person AS lepp
    ON pp.person_id = lepp.person_id
