CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_SMOKING_STATUS (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    SMOKING_STATUS VARCHAR, -- Current smoking status (Current Smoker, Ex Smoker, Never Smoked, Unknown)
    LATEST_SMOKING_DATE DATE, -- Date of the latest smoking status record
    EARLIEST_SMOKING_DATE DATE, -- Date of the earliest smoking status record
    LATEST_CONCEPT_CODE VARCHAR, -- Concept code of the latest smoking status
    LATEST_CODE_DESCRIPTION VARCHAR, -- Description of the latest smoking status
    LATEST_CLUSTER_ID VARCHAR, -- Cluster ID of the latest smoking status (SMOK_COD, LSMOK_COD, EXSMOK_COD, NSMOK_COD)
    ALL_SMOKING_CONCEPT_CODES ARRAY, -- All smoking concept codes for this person
    ALL_SMOKING_CONCEPT_DISPLAYS ARRAY, -- All smoking concept display terms for this person
    LAST_REFRESH_DATE TIMESTAMP_NTZ -- When this record was last refreshed
)
COMMENT = 'Fact table containing the latest smoking status for each person using QOF definitions. Determines smoking status based on the most recent smoking-related observation, with priority given to specific status codes (LSMOK_COD, EXSMOK_COD, NSMOK_COD) over general smoking codes (SMOK_COD). Derived from INTERMEDIATE_SMOKING_ALL.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH LatestSmokingRecord AS (
    -- Get the latest smoking record for each person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CODE_DESCRIPTION,
        SOURCE_CLUSTER_ID,
        IS_SMOKER_CODE,
        IS_EX_SMOKER_CODE,
        IS_NEVER_SMOKED_CODE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_ALL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
),
SmokingHistory AS (
    -- Get earliest date and all codes for each person
    SELECT
        PERSON_ID,
        MIN(CLINICAL_EFFECTIVE_DATE) AS EARLIEST_SMOKING_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_SMOKING_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_SMOKING_CONCEPT_DISPLAYS
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_ALL
    GROUP BY PERSON_ID
)
SELECT
    lsr.PERSON_ID,
    lsr.SK_PATIENT_ID,
    -- Determine smoking status based on the latest record's code flags
    CASE
        WHEN lsr.IS_SMOKER_CODE THEN 'Current Smoker'
        WHEN lsr.IS_EX_SMOKER_CODE THEN 'Ex Smoker'
        WHEN lsr.IS_NEVER_SMOKED_CODE THEN 'Never Smoked'
        ELSE 'Unknown'
    END AS SMOKING_STATUS,
    lsr.CLINICAL_EFFECTIVE_DATE AS LATEST_SMOKING_DATE,
    sh.EARLIEST_SMOKING_DATE,
    lsr.CONCEPT_CODE AS LATEST_CONCEPT_CODE,
    lsr.CODE_DESCRIPTION AS LATEST_CODE_DESCRIPTION,
    lsr.SOURCE_CLUSTER_ID AS LATEST_CLUSTER_ID,
    sh.ALL_SMOKING_CONCEPT_CODES,
    sh.ALL_SMOKING_CONCEPT_DISPLAYS,
    CURRENT_TIMESTAMP() AS LAST_REFRESH_DATE
FROM LatestSmokingRecord lsr
LEFT JOIN SmokingHistory sh
    ON lsr.PERSON_ID = sh.PERSON_ID;
