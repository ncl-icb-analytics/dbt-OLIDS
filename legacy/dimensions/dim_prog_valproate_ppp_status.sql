CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_PPP_STATUS (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    HAS_PPP_EVENT BOOLEAN COMMENT 'Always TRUE, indicating at least one PPP-related event meeting criteria',
    EARLIEST_PPP_EVENT_DATE DATE COMMENT 'Earliest date of any PPP-related event for the person',
    LATEST_PPP_EVENT_DATE DATE COMMENT 'Latest date of any PPP-related event for the person',
    LATEST_PPP_OBSERVATION_ID VARCHAR COMMENT 'Observation ID for the most recent PPP event',
    LATEST_PPP_CONCEPT_CODE VARCHAR COMMENT 'Medical concept code for the most recent PPP event',
    LATEST_PPP_CONCEPT_DISPLAY VARCHAR COMMENT 'Display term for the most recent PPP concept code',
    IS_CURRENTLY_PPP_ENROLLED BOOLEAN COMMENT 'TRUE if most recent PPP status indicates enrollment',
    IS_PPP_NON_ENROLLED BOOLEAN COMMENT 'TRUE if most recent PPP status indicates discontinued/not needed/declined',
    CURRENT_PPP_STATUS_DESCRIPTION VARCHAR COMMENT 'Human-readable description of current PPP status',
    CURRENT_PPP_STATUS_WITH_DATE VARCHAR COMMENT 'PPP status description concatenated with formatted date',
    ALL_PPP_OBSERVATION_IDS ARRAY COMMENT 'Array of unique observation IDs related to PPP events',
    ALL_PPP_CONCEPT_CODES ARRAY COMMENT 'Array of all distinct PPP-related medical concept codes found',
    ALL_PPP_CONCEPT_DISPLAYS ARRAY COMMENT 'Array of display terms for the PPP codes',
    ALL_PPP_CODE_CATEGORIES_APPLIED ARRAY COMMENT 'Array of code categories applied (will contain various PPP categories)'
)
COMMENT = 'Dimension table aggregating Pregnancy Prevention Programme (PPP) events for each person.

Sources from INTERMEDIATE_PPP_STATUS_ALL to provide:
  - Analytics-ready person-level PPP status
  - Historical and current status information
  - Latest event details with formatted dates
  - Comprehensive event aggregations

Status categories:
  - Enrolled
  - Discontinued  
  - Not needed
  - Declined'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePPPObservations AS (
    -- Get all PPP events from intermediate table with patient keys
    SELECT
        ppp.PERSON_ID,
        PAT."sk_patient_id" AS SK_PATIENT_ID,
        ppp.PPP_OBSERVATION_ID AS OBSERVATION_ID,
        ppp.PPP_EVENT_DATE,
        ppp.PPP_CONCEPT_CODE,
        ppp.PPP_CONCEPT_DISPLAY,
        ppp.PPP_CATEGORIES,
        CASE WHEN ppp.PPP_STATUS_DESCRIPTION = 'Yes - PPP enrolled' THEN TRUE ELSE FALSE END AS IS_PPP_ENROLLED,
        ppp.PPP_STATUS_DESCRIPTION
    FROM
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PPP_STATUS_ALL ppp
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON ppp.PERSON_ID = PP."person_id"
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS PAT
        ON PP."patient_id" = PAT."id"
),
LatestPPPStatus AS (
    -- Get the most recent PPP event for each person
    SELECT 
        PERSON_ID,
        OBSERVATION_ID,
        PPP_EVENT_DATE,
        PPP_CONCEPT_CODE,
        PPP_CONCEPT_DISPLAY,
        IS_PPP_ENROLLED,
        PPP_STATUS_DESCRIPTION
    FROM BasePPPObservations
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY PPP_EVENT_DATE DESC) = 1
),
PersonLevelPPPAggregation AS (
    -- Aggregates PPP information for each person from intermediate data
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) AS SK_PATIENT_ID,
        MIN(PPP_EVENT_DATE) AS EARLIEST_PPP_EVENT_DATE,
        MAX(PPP_EVENT_DATE) AS LATEST_PPP_EVENT_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_PPP_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT PPP_CONCEPT_CODE) WITHIN GROUP (ORDER BY PPP_CONCEPT_CODE) AS ALL_PPP_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT PPP_CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY PPP_CONCEPT_DISPLAY) AS ALL_PPP_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT PPP_CATEGORIES[0]::STRING) WITHIN GROUP (ORDER BY PPP_CATEGORIES[0]::STRING) AS ALL_PPP_CODE_CATEGORIES_APPLIED
    FROM BasePPPObservations
    GROUP BY PERSON_ID
)
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    TRUE AS HAS_PPP_EVENT,
    pla.EARLIEST_PPP_EVENT_DATE,
    pla.LATEST_PPP_EVENT_DATE,
    latest.OBSERVATION_ID AS LATEST_PPP_OBSERVATION_ID,
    latest.PPP_CONCEPT_CODE AS LATEST_PPP_CONCEPT_CODE,
    latest.PPP_CONCEPT_DISPLAY AS LATEST_PPP_CONCEPT_DISPLAY,
    latest.IS_PPP_ENROLLED AS IS_CURRENTLY_PPP_ENROLLED,
    NOT latest.IS_PPP_ENROLLED AS IS_PPP_NON_ENROLLED,
    latest.PPP_STATUS_DESCRIPTION AS CURRENT_PPP_STATUS_DESCRIPTION,
    latest.PPP_STATUS_DESCRIPTION || ' (' || TO_CHAR(latest.PPP_EVENT_DATE, 'DD/MM/YYYY') || ')' AS CURRENT_PPP_STATUS_WITH_DATE,
    pla.ALL_PPP_OBSERVATION_IDS,
    pla.ALL_PPP_CONCEPT_CODES,
    pla.ALL_PPP_CONCEPT_DISPLAYS,
    pla.ALL_PPP_CODE_CATEGORIES_APPLIED
FROM PersonLevelPPPAggregation pla
JOIN LatestPPPStatus latest ON pla.PERSON_ID = latest.PERSON_ID; 