CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_PSYCHIATRY (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person, sourced from PATIENT_PERSON table',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient from PATIENT table, used for joining with other patient-related dimensions',
    HAS_PSYCH_EVENT BOOLEAN COMMENT 'Boolean flag indicating presence of psychiatry events. Always TRUE in this table as only patients with psychiatric events are included',
    EARLIEST_PSYCH_EVENT_DATE DATE COMMENT 'Date of the first recorded psychiatry-related event for the person. Used for temporal analysis and establishing onset of psychiatric care',
    LATEST_PSYCH_EVENT_DATE DATE COMMENT 'Date of the most recent psychiatry-related event. Important for identifying currency of psychiatric care',
    ALL_PSYCH_OBSERVATION_IDS ARRAY COMMENT 'Array of observation IDs from OBSERVATION table, ordered ascending. Each ID represents a distinct psychiatric event or diagnosis',
    ALL_PSYCH_CONCEPT_CODES ARRAY COMMENT 'Array of unique medical concept codes related to psychiatric conditions or treatments, ordered ascending. Links to MAPPED_CONCEPTS table',
    ALL_PSYCH_CONCEPT_DISPLAYS ARRAY COMMENT 'Array of human-readable descriptions for each psychiatric concept code, ordered ascending. Sourced from MAPPED_CONCEPTS.CODE_DESCRIPTION',
    ALL_PSYCH_CODE_CATEGORIES_APPLIED ARRAY COMMENT 'Array containing the category "PSYCH" for all records. Used for filtering and validation of psychiatric events'
)
COMMENT = 'Dimension table for the Valproate Programme focusing on psychiatric events and diagnoses. This table:
- Aggregates all psychiatry-related events from the OBSERVATION table
- Uses codes defined in VALPROATE_PROG_CODES with category "PSYCH"
- Provides a comprehensive view of a patient''s psychiatric history
- Supports analysis of Valproate prescribing patterns in psychiatric care
- Has no temporal restrictions (LOOKBACK_YEARS_OFFSET is NULL for all PSYCH codes)
Key relationships:
- Links to fact tables via PERSON_ID and SK_PATIENT_ID
- Concept codes map to MAPPED_CONCEPTS for standardised terminology
- Observation IDs reference back to source OBSERVATION table'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePsychObservations AS (
    -- Initial extraction of psychiatric observations and related patient data
    -- Purpose: Identifies all psychiatric events and links them to patient identifiers
    -- Note: No temporal filtering is applied as LOOKBACK_YEARS_OFFSET is NULL for psychiatric codes
    SELECT
        PP."person_id" AS PERSON_ID,
        PAT."sk_patient_id" AS SK_PATIENT_ID,
        O."id" AS OBSERVATION_ID,
        O."clinical_effective_date"::DATE AS PSYCH_EVENT_DATE,
        MC.CONCEPT_CODE AS PSYCH_CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS PSYCH_CONCEPT_DISPLAY,
        VPC.CODE_CATEGORY AS PSYCH_CODE_CATEGORY -- Will always be 'PSYCH' due to WHERE clause filter
    FROM
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID  -- Links observations to standardised concept codes
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.VALPROATE_PROG_CODES AS VPC
        ON MC.CONCEPT_CODE = VPC.CODE  -- Filters for relevant psychiatric codes
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"  -- Links observations to person identifiers
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS PAT
        ON PP."patient_id" = PAT."id"  -- Obtains surrogate keys for patient linking
    WHERE
        VPC.CODE_CATEGORY = 'PSYCH'
        AND (
            VPC.LOOKBACK_YEARS_OFFSET IS NULL OR  -- Always TRUE for PSYCH category
            O."clinical_effective_date"::DATE >= DATEADD(YEAR, VPC.LOOKBACK_YEARS_OFFSET, CURRENT_DATE())
        )
),
PersonLevelPsychAggregation AS (
    -- Aggregates all psychiatric events to create a person-level summary
    -- Purpose: Transforms event-level data into a comprehensive patient psychiatric profile
    -- Note: Uses ordered aggregation to ensure consistent array contents
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,  -- Single SK per person
        MIN(PSYCH_EVENT_DATE) AS EARLIEST_PSYCH_EVENT_DATE,  -- First recorded psychiatric event
        MAX(PSYCH_EVENT_DATE) AS LATEST_PSYCH_EVENT_DATE,    -- Most recent psychiatric event
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_PSYCH_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT PSYCH_CONCEPT_CODE) WITHIN GROUP (ORDER BY PSYCH_CONCEPT_CODE) AS ALL_PSYCH_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT PSYCH_CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY PSYCH_CONCEPT_DISPLAY) AS ALL_PSYCH_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT PSYCH_CODE_CATEGORY) WITHIN GROUP (ORDER BY PSYCH_CODE_CATEGORY) AS ALL_PSYCH_CODE_CATEGORIES_APPLIED
    FROM BasePsychObservations
    GROUP BY PERSON_ID
)
-- Final selection creates the dimension table entries
-- Adds the constant TRUE flag for HAS_PSYCH_EVENT as this table only contains
-- patients with at least one qualifying psychiatric event
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    TRUE AS HAS_PSYCH_EVENT,
    pla.EARLIEST_PSYCH_EVENT_DATE,
    pla.LATEST_PSYCH_EVENT_DATE,
    pla.ALL_PSYCH_OBSERVATION_IDS,
    pla.ALL_PSYCH_CONCEPT_CODES,
    pla.ALL_PSYCH_CONCEPT_DISPLAYS,
    pla.ALL_PSYCH_CODE_CATEGORIES_APPLIED
FROM PersonLevelPsychAggregation pla; 