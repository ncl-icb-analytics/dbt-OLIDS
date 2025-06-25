CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_65 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_MODERATE_HIGH_BMI BOOLEAN, -- Flag indicating if person has moderate-high BMI based on ethnicity
    IS_BAME BOOLEAN, -- Flag indicating if person is from BAME ethnicity
    LATEST_BMI_DATE DATE, -- Date of most recent BMI measurement
    LATEST_BMI_VALUE NUMBER, -- Most recent BMI value
    LATEST_HBA1C_DATE DATE, -- Date of most recent HbA1c reading (if any)
    LATEST_HBA1C_VALUE NUMBER, -- Most recent HbA1c value (if any)
    ALL_BMI_CODES ARRAY, -- Array of all BMI codes
    ALL_BMI_DISPLAYS ARRAY -- Array of all BMI display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator DM_65: Patients who meet ALL of the following criteria:
1. Moderate-high BMI based on ethnicity:
   - BMI between 27.5 and 32.5 for BAME patients
   - BMI between 30 and 35 for non-BAME patients
2. No HbA1c reading in the last 24 months

Exclusions:
- Patients on any LTC register
- Patients with NHS health check in last 24 months
- Patients under 17 years of age

Only includes patients who meet all inclusion criteria and no exclusion criteria.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients over 17
    -- Note: Base population already excludes those on LTC registers and with NHS health check in last 24 months
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
    WHERE age.AGE >= 17
),
BAMEPopulation AS (
    -- Get patients from BAME ethnicity
    SELECT DISTINCT
        PERSON_ID,
        TRUE AS IS_BAME
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'ETHNICITY_BAME'
    EXCEPT
    SELECT DISTINCT
        PERSON_ID,
        TRUE AS IS_BAME
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID IN ('ETHNICITY_WHITE_BRITISH', 'DIABETES_EXCLUDED_ETHNICITY')
),
BMIMeasurements AS (
    -- Get all BMI measurements
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        RESULT_VALUE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'BMI_MEASUREMENT'
        AND RESULT_VALUE > 0
),
LatestBMI AS (
    -- Get the most recent BMI measurement for each person
    SELECT
        r.PERSON_ID,
        r.SK_PATIENT_ID,
        r.CLINICAL_EFFECTIVE_DATE AS LATEST_BMI_DATE,
        r.RESULT_VALUE AS LATEST_BMI_VALUE,
        (
            SELECT ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE)
            FROM BMIMeasurements b2
            WHERE b2.PERSON_ID = r.PERSON_ID
        ) AS ALL_BMI_CODES,
        (
            SELECT ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY)
            FROM BMIMeasurements b2
            WHERE b2.PERSON_ID = r.PERSON_ID
        ) AS ALL_BMI_DISPLAYS
    FROM BMIMeasurements r
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
),
RecentHBA1C AS (
    -- Get patients with HbA1c in last 24 months (for exclusion)
    SELECT DISTINCT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE AS LATEST_HBA1C_DATE,
        RESULT_VALUE AS LATEST_HBA1C_VALUE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'HBA1C_LEVEL'
        AND RESULT_VALUE > 0
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(year, -2, CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE
        WHEN (bame.IS_BAME = TRUE AND bmi.LATEST_BMI_VALUE >= 27.5 AND bmi.LATEST_BMI_VALUE < 32.5) OR
             (bame.IS_BAME IS NULL AND bmi.LATEST_BMI_VALUE >= 30 AND bmi.LATEST_BMI_VALUE < 35) THEN TRUE
        ELSE FALSE
    END AS HAS_MODERATE_HIGH_BMI,
    COALESCE(bame.IS_BAME, FALSE) AS IS_BAME,
    bmi.LATEST_BMI_DATE,
    bmi.LATEST_BMI_VALUE,
    hba1c.LATEST_HBA1C_DATE,
    hba1c.LATEST_HBA1C_VALUE,
    bmi.ALL_BMI_CODES,
    bmi.ALL_BMI_DISPLAYS
FROM BasePopulation bp
LEFT JOIN BAMEPopulation bame
    USING (PERSON_ID)
LEFT JOIN LatestBMI bmi
    USING (PERSON_ID)
LEFT JOIN RecentHBA1C hba1c
    USING (PERSON_ID)
WHERE ((bame.IS_BAME = TRUE AND bmi.LATEST_BMI_VALUE >= 27.5 AND bmi.LATEST_BMI_VALUE < 32.5) OR
       (bame.IS_BAME IS NULL AND bmi.LATEST_BMI_VALUE >= 30 AND bmi.LATEST_BMI_VALUE < 35))
    AND hba1c.PERSON_ID IS NULL;
