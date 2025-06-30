-- ==========================================================================
-- Person Dimension Dynamic Table - Simplified
-- Aggregates person-to-patient relationships and practice associations
-- Uses arrays to store multiple patient IDs and practice information per person
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_IDS ARRAY COMMENT 'Array of all surrogate patient keys associated with this person',
    PATIENT_IDS ARRAY COMMENT 'Array of all patient IDs associated with this person',
    PRACTICE_IDS ARRAY COMMENT 'Array of all practice IDs this person has been registered with',
    PRACTICE_CODES ARRAY COMMENT 'Array of all practice codes this person has been registered with',
    PRACTICE_NAMES ARRAY COMMENT 'Array of all practice names this person has been registered with',
    CURRENT_PRACTICE_ID VARCHAR COMMENT 'Current/most recent practice ID for this person',
    CURRENT_PRACTICE_CODE VARCHAR COMMENT 'Current/most recent practice code for this person',
    CURRENT_PRACTICE_NAME VARCHAR COMMENT 'Current/most recent practice name for this person',
    TOTAL_PATIENTS NUMBER COMMENT 'Number of patient records associated with this person',
    TOTAL_PRACTICES NUMBER COMMENT 'Number of practices this person has been registered with'
)
COMMENT = 'Simplified person dimension providing aggregated patient and practice relationships. Uses arrays to efficiently store multiple patient IDs and practice associations per person.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH person_patients AS (
    -- Get all patient relationships for each person
    SELECT
        pp."person_id" AS PERSON_ID,
        ARRAY_AGG(DISTINCT p."sk_patient_id") AS SK_PATIENT_IDS,
        ARRAY_AGG(DISTINCT p."id") AS PATIENT_IDS,
        COUNT(DISTINCT p."id") AS TOTAL_PATIENTS
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
        ON pp."patient_id" = p."id"
    GROUP BY pp."person_id"
),
person_practices AS (
    -- Get all practice relationships from the historical practice dimension
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT PRACTICE_ID) AS PRACTICE_IDS,
        ARRAY_AGG(DISTINCT PRACTICE_CODE) AS PRACTICE_CODES,
        ARRAY_AGG(DISTINCT PRACTICE_NAME) AS PRACTICE_NAMES,
        COUNT(DISTINCT PRACTICE_ID) AS TOTAL_PRACTICES
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_HISTORICAL_PRACTICE
    GROUP BY PERSON_ID
),
current_practices AS (
    -- Get the current practice for each person
    SELECT
        PERSON_ID,
        PRACTICE_ID AS CURRENT_PRACTICE_ID,
        PRACTICE_CODE AS CURRENT_PRACTICE_CODE,
        PRACTICE_NAME AS CURRENT_PRACTICE_NAME
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_HISTORICAL_PRACTICE
    WHERE IS_CURRENT_PRACTICE = TRUE
)
-- Final aggregation
SELECT
    pp.PERSON_ID,
    pp.SK_PATIENT_IDS,
    pp.PATIENT_IDS,
    COALESCE(pr.PRACTICE_IDS, ARRAY_CONSTRUCT()) AS PRACTICE_IDS,
    COALESCE(pr.PRACTICE_CODES, ARRAY_CONSTRUCT()) AS PRACTICE_CODES,
    COALESCE(pr.PRACTICE_NAMES, ARRAY_CONSTRUCT()) AS PRACTICE_NAMES,
    cp.CURRENT_PRACTICE_ID,
    cp.CURRENT_PRACTICE_CODE,
    cp.CURRENT_PRACTICE_NAME,
    pp.TOTAL_PATIENTS,
    COALESCE(pr.TOTAL_PRACTICES, 0) AS TOTAL_PRACTICES
FROM person_patients pp
LEFT JOIN person_practices pr ON pp.PERSON_ID = pr.PERSON_ID
LEFT JOIN current_practices cp ON pp.PERSON_ID = cp.PERSON_ID
