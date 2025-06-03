CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_61 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_CKD BOOLEAN, -- Flag indicating if person has two consecutive eGFR readings < 60
    LATEST_EGFR_DATE DATE, -- Date of most recent eGFR reading
    PREVIOUS_EGFR_DATE DATE, -- Date of previous eGFR reading
    LATEST_EGFR_VALUE NUMBER, -- Most recent eGFR value
    PREVIOUS_EGFR_VALUE NUMBER, -- Previous eGFR value
    ALL_EGFR_CODES ARRAY, -- Array of all eGFR codes
    ALL_EGFR_DISPLAYS ARRAY -- Array of all eGFR display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator CKD_61: Patients with two consecutive eGFR readings below 60. Only includes patients who meet all criteria.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients over 17
    -- Note: Base population already excludes those on CKD and Diabetes registers
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
    WHERE age.AGE >= 17
),
EGFRReadings AS (
    -- Get all eGFR readings with values > 0
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        RESULT_VALUE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'EGFR_TESTING'
        AND RESULT_VALUE > 0
),
EGFRRanked AS (
    -- Rank eGFR readings by date for each person
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) AS READING_RANK
    FROM EGFRReadings
),
EGFRCounts AS (
    -- Count readings per person to ensure at least 2
    SELECT
        PERSON_ID,
        COUNT(*) AS READING_COUNT
    FROM EGFRReadings
    GROUP BY PERSON_ID
    HAVING COUNT(*) > 1
),
EGFRWithLags AS (
    -- Get the two most recent readings with their lags
    SELECT
        er.PERSON_ID,
        er.SK_PATIENT_ID,
        er.CLINICAL_EFFECTIVE_DATE AS LATEST_EGFR_DATE,
        LAG(er.CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY er.PERSON_ID ORDER BY er.CLINICAL_EFFECTIVE_DATE DESC) AS PREVIOUS_EGFR_DATE,
        er.RESULT_VALUE AS LATEST_EGFR_VALUE,
        LAG(er.RESULT_VALUE) OVER (PARTITION BY er.PERSON_ID ORDER BY er.CLINICAL_EFFECTIVE_DATE DESC) AS PREVIOUS_EGFR_VALUE
    FROM EGFRRanked er
    JOIN EGFRCounts ec
        USING (PERSON_ID)
    WHERE er.READING_RANK <= 2
    QUALIFY er.READING_RANK = 1
),
EGFRCodes AS (
    -- Get all codes and displays for each person
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_EGFR_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_EGFR_DISPLAYS
    FROM EGFRReadings
    GROUP BY PERSON_ID
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE 
        WHEN ceg.LATEST_EGFR_VALUE < 60 AND ceg.PREVIOUS_EGFR_VALUE < 60 THEN TRUE
        ELSE FALSE
    END AS HAS_CKD,
    ceg.LATEST_EGFR_DATE,
    ceg.PREVIOUS_EGFR_DATE,
    ceg.LATEST_EGFR_VALUE,
    ceg.PREVIOUS_EGFR_VALUE,
    codes.ALL_EGFR_CODES,
    codes.ALL_EGFR_DISPLAYS
FROM BasePopulation bp
LEFT JOIN EGFRWithLags ceg
    USING (PERSON_ID)
LEFT JOIN EGFRCodes codes
    USING (PERSON_ID)
WHERE ceg.LATEST_EGFR_VALUE < 60 
    AND ceg.PREVIOUS_EGFR_VALUE < 60; 