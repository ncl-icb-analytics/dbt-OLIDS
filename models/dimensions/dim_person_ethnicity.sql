-- ==========================================================================
-- Dimension Dynamic Table holding the latest ethnicity record for ALL persons.
-- Starts from PATIENT_PERSON and LEFT JOINs the latest ethnicity record if available.
-- Ethnicity fields display 'Not Recorded' for persons with no recorded ethnicity.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_ETHNICITY (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID NUMBER, -- Surrogate key for the patient from the PATIENT table
    LATEST_ETHNICITY_DATE DATE, -- Date of the most recent ethnicity observation for the person; NULL if not recorded
    CONCEPT_ID VARCHAR, -- Concept ID of the latest ethnicity observation; 'Not Recorded' if NULL
    SNOMED_CODE VARCHAR, -- SNOMED code of the latest ethnicity observation; 'Not Recorded' if NULL
    TERM VARCHAR, -- Term/description of the latest ethnicity observation; 'Not Recorded' if NULL
    ETHNICITY_CATEGORY VARCHAR, -- Broad ethnicity category (e.g., White, Asian); 'Not Recorded' if NULL
    ETHNICITY_SUBCATEGORY VARCHAR, -- More specific ethnicity subcategory; 'Not Recorded' if NULL
    ETHNICITY_GRANULAR VARCHAR -- Most granular ethnicity detail available; 'Not Recorded' if NULL
)
COMMENT = 'Dimension table providing the latest recorded ethnicity for every person. If no ethnicity is recorded for a person, ethnicity-related fields default to \'Not Recorded\'.'
TARGET_LAG = '4 hours' -- Should be same or longer than INTERMEDIATE_PERSON_ETHNICITY_ALL lag
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LatestEthnicityPerPerson AS (
    -- Identifies the single most recent ethnicity record for each person from the INTERMEDIATE_PERSON_ETHNICITY_ALL table.
    -- Uses ROW_NUMBER() partitioned by person_id, ordered by clinical_effective_date (desc) and observation_lds_id (desc as tie-breaker).
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
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_ETHNICITY_ALL pea -- Read from the intermediate table
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
    pp."person_id",
    p."sk_patient_id", -- Get sk_patient_id from PATIENT table
    -- Ethnicity fields from the latest record, using COALESCE for NULLs
    lepp.clinical_effective_date AS latest_ethnicity_date, -- Date remains NULL if no record
    COALESCE(lepp.concept_id, 'Not Recorded') AS concept_id,
    COALESCE(lepp.snomed_code, 'Not Recorded') AS snomed_code,
    COALESCE(lepp.term, 'Not Recorded') AS term,
    COALESCE(lepp.ethnicity_category, 'Not Recorded') AS ethnicity_category,
    COALESCE(lepp.ethnicity_subcategory, 'Not Recorded') AS ethnicity_subcategory,
    COALESCE(lepp.ethnicity_granular, 'Not Recorded') AS ethnicity_granular
FROM
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS pp -- Start with all persons
LEFT JOIN -- Use LEFT JOIN to keep persons even if no PATIENT record (unlikely but safe)
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS p
    ON pp."patient_id" = p."id"
LEFT JOIN -- Use LEFT JOIN to keep all persons, regardless of whether they have an ethnicity record
    LatestEthnicityPerPerson lepp ON pp."person_id" = lepp.person_id;

