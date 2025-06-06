CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_63 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_ELEVATED_UACR BOOLEAN, -- Flag indicating if person has latest UACR reading > 70
    LATEST_UACR_DATE DATE, -- Date of most recent UACR reading
    LATEST_UACR_VALUE NUMBER, -- Most recent UACR value
    ALL_UACR_CODES ARRAY, -- Array of all UACR codes
    ALL_UACR_DISPLAYS ARRAY -- Array of all UACR display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator CKD_63: Patients with latest UACR reading above 70, excluding those in CKD_62. Only includes patients who meet all criteria.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients over 17
    -- Excludes those on CKD and Diabetes registers, and those in CKD_62
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_62 ckd62
        USING (PERSON_ID)
    WHERE age.AGE >= 17
        AND ckd62.PERSON_ID IS NULL -- Exclude patients in CKD_62
),
UACRReadings AS (
    -- Get all UACR readings with values > 0
    -- Take max value per day to handle multiple readings
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        MAX(RESULT_VALUE) AS RESULT_VALUE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'UACR_TESTING'
        AND RESULT_VALUE > 0
    GROUP BY 
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY
),
LatestUACR AS (
    -- Get the most recent UACR reading for each person
    SELECT
        ur.PERSON_ID,
        ur.SK_PATIENT_ID,
        ur.CLINICAL_EFFECTIVE_DATE AS LATEST_UACR_DATE,
        ur.RESULT_VALUE AS LATEST_UACR_VALUE
    FROM UACRReadings ur
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ur.PERSON_ID ORDER BY ur.CLINICAL_EFFECTIVE_DATE DESC) = 1
),
UACRCodes AS (
    -- Get all codes and displays for each person
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_UACR_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_UACR_DISPLAYS
    FROM UACRReadings
    GROUP BY PERSON_ID
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE 
        WHEN ceg.LATEST_UACR_VALUE > 70 THEN TRUE
        ELSE FALSE
    END AS HAS_ELEVATED_UACR,
    ceg.LATEST_UACR_DATE,
    ceg.LATEST_UACR_VALUE,
    codes.ALL_UACR_CODES,
    codes.ALL_UACR_DISPLAYS
FROM BasePopulation bp
LEFT JOIN LatestUACR ceg
    USING (PERSON_ID)
LEFT JOIN UACRCodes codes
    USING (PERSON_ID)
WHERE ceg.LATEST_UACR_VALUE > 70; 