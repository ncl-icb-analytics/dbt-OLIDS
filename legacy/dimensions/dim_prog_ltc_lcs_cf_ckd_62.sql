CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_CKD_62 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_ELEVATED_UACR BOOLEAN, -- Flag indicating if person has two consecutive UACR readings > 4
    LATEST_UACR_DATE DATE, -- Date of most recent UACR reading
    PREVIOUS_UACR_DATE DATE, -- Date of previous UACR reading
    LATEST_UACR_VALUE NUMBER, -- Most recent UACR value
    PREVIOUS_UACR_VALUE NUMBER, -- Previous UACR value
    ALL_UACR_CODES ARRAY, -- Array of all UACR codes
    ALL_UACR_DISPLAYS ARRAY -- Array of all UACR display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator CKD_62: Patients with two consecutive UACR readings above 4. Only includes patients who meet all criteria.'
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
UACRWithAdjacentCheck AS (
    -- Check for same results on adjacent days
    SELECT
        *,
        CASE
            WHEN CLINICAL_EFFECTIVE_DATE + 1 = LAG(CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC)
            AND RESULT_VALUE = LAG(RESULT_VALUE) OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC)
            THEN 'EXCLUDE'
            ELSE 'INCLUDE'
        END AS ADJACENT_DAY_CHECK
    FROM UACRReadings
),
UACRFiltered AS (
    -- Remove adjacent day duplicates
    SELECT *
    FROM UACRWithAdjacentCheck
    WHERE ADJACENT_DAY_CHECK = 'INCLUDE'
),
UACRRanked AS (
    -- Rank UACR readings by date for each person
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) AS READING_RANK
    FROM UACRFiltered
),
UACRCounts AS (
    -- Count readings per person to ensure at least 2
    SELECT
        PERSON_ID,
        COUNT(*) AS READING_COUNT
    FROM UACRFiltered
    GROUP BY PERSON_ID
    HAVING COUNT(*) > 1
),
UACRWithLags AS (
    -- Get the two most recent readings with their lags
    SELECT
        ur.PERSON_ID,
        ur.SK_PATIENT_ID,
        ur.CLINICAL_EFFECTIVE_DATE AS LATEST_UACR_DATE,
        LAG(ur.CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY ur.PERSON_ID ORDER BY ur.CLINICAL_EFFECTIVE_DATE DESC) AS PREVIOUS_UACR_DATE,
        ur.RESULT_VALUE AS LATEST_UACR_VALUE,
        LAG(ur.RESULT_VALUE) OVER (PARTITION BY ur.PERSON_ID ORDER BY ur.CLINICAL_EFFECTIVE_DATE DESC) AS PREVIOUS_UACR_VALUE
    FROM UACRRanked ur
    JOIN UACRCounts uc
        USING (PERSON_ID)
    WHERE ur.READING_RANK <= 2
    QUALIFY ur.READING_RANK = 1
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
        WHEN ceg.LATEST_UACR_VALUE > 4 AND ceg.PREVIOUS_UACR_VALUE > 4 THEN TRUE
        ELSE FALSE
    END AS HAS_ELEVATED_UACR,
    ceg.LATEST_UACR_DATE,
    ceg.PREVIOUS_UACR_DATE,
    ceg.LATEST_UACR_VALUE,
    ceg.PREVIOUS_UACR_VALUE,
    codes.ALL_UACR_CODES,
    codes.ALL_UACR_DISPLAYS
FROM BasePopulation bp
LEFT JOIN UACRWithLags ceg
    USING (PERSON_ID)
LEFT JOIN UACRCodes codes
    USING (PERSON_ID)
WHERE ceg.LATEST_UACR_VALUE > 4
    AND ceg.PREVIOUS_UACR_VALUE > 4;
