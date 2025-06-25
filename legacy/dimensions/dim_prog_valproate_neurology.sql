CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_NEUROLOGY (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    HAS_NEUROLOGY_EVENT BOOLEAN COMMENT 'Always TRUE, indicating at least one neurology-related event meeting criteria',
    EARLIEST_NEUROLOGY_EVENT_DATE DATE COMMENT 'Earliest date of any neurology-related event for the person',
    LATEST_NEUROLOGY_EVENT_DATE DATE COMMENT 'Latest date of any neurology-related event for the person',
    ALL_NEUROLOGY_OBSERVATION_IDS ARRAY COMMENT 'Array of unique observation IDs related to neurology events',
    ALL_NEUROLOGY_CONCEPT_CODES ARRAY COMMENT 'Array of all distinct neurology-related medical concept codes found',
    ALL_NEUROLOGY_CONCEPT_DISPLAYS ARRAY COMMENT 'Array of display terms for the neurology codes (from MAPPED_CONCEPTS.CODE_DESCRIPTION)',
    ALL_NEUROLOGY_CODE_CATEGORIES_APPLIED ARRAY COMMENT 'Array of code categories applied (will contain \'-NEUROLOGY\'-)'
)
COMMENT = 'Dimension table aggregating neurology-related events for each person, using MAPPED_CONCEPTS, OBSERVATION, and VALPROATE_PROG_CODES (category NEUROLOGY). These codes typically indicate conditions for which Valproate might be prescribed.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseNeurologyObservations AS (
    -- Selects neurology-related observations.
    -- LOOKBACK_YEARS_OFFSET is NULL for all NEUROLOGY codes, so all historical records are considered.
    SELECT
        PP."person_id" AS PERSON_ID,
        PAT."sk_patient_id" AS SK_PATIENT_ID,
        O."id" AS OBSERVATION_ID,
        O."clinical_effective_date"::DATE AS NEUROLOGY_EVENT_DATE,
        MC.CONCEPT_CODE AS NEUROLOGY_CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS NEUROLOGY_CONCEPT_DISPLAY,
        VPC.CODE_CATEGORY AS NEUROLOGY_CODE_CATEGORY -- This will be 'NEUROLOGY'
    FROM
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.VALPROATE_PROG_CODES AS VPC
        ON MC.CONCEPT_CODE = VPC.CODE
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS PAT
        ON PP."patient_id" = PAT."id"
    WHERE
        VPC.CODE_CATEGORY = 'NEUROLOGY'
        AND (
            VPC.LOOKBACK_YEARS_OFFSET IS NULL OR
            O."clinical_effective_date"::DATE >= DATEADD(YEAR, VPC.LOOKBACK_YEARS_OFFSET, CURRENT_DATE())
        )
),
PersonLevelNeurologyAggregation AS (
    -- Aggregates neurology information for each person.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        MIN(NEUROLOGY_EVENT_DATE) AS EARLIEST_NEUROLOGY_EVENT_DATE,
        MAX(NEUROLOGY_EVENT_DATE) AS LATEST_NEUROLOGY_EVENT_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_NEUROLOGY_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT NEUROLOGY_CONCEPT_CODE) WITHIN GROUP (ORDER BY NEUROLOGY_CONCEPT_CODE) AS ALL_NEUROLOGY_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT NEUROLOGY_CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY NEUROLOGY_CONCEPT_DISPLAY) AS ALL_NEUROLOGY_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT NEUROLOGY_CODE_CATEGORY) WITHIN GROUP (ORDER BY NEUROLOGY_CODE_CATEGORY) AS ALL_NEUROLOGY_CODE_CATEGORIES_APPLIED
    FROM BaseNeurologyObservations
    GROUP BY PERSON_ID
)
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    TRUE AS HAS_NEUROLOGY_EVENT,
    pla.EARLIEST_NEUROLOGY_EVENT_DATE,
    pla.LATEST_NEUROLOGY_EVENT_DATE,
    pla.ALL_NEUROLOGY_OBSERVATION_IDS,
    pla.ALL_NEUROLOGY_CONCEPT_CODES,
    pla.ALL_NEUROLOGY_CONCEPT_DISPLAYS,
    pla.ALL_NEUROLOGY_CODE_CATEGORIES_APPLIED
FROM PersonLevelNeurologyAggregation pla;
