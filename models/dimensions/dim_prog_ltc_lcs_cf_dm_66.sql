CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_66 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_ELEVATED_HBA1C BOOLEAN, -- Flag indicating if person has HbA1c between 42 and 46 mmol/mol
    LATEST_HBA1C_DATE DATE, -- Date of most recent HbA1c reading
    LATEST_HBA1C_VALUE NUMBER, -- Most recent HbA1c value
    ALL_HBA1C_CODES ARRAY, -- Array of all HbA1c codes
    ALL_HBA1C_DISPLAYS ARRAY -- Array of all HbA1c display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator DM_66: Patients who meet ALL of the following criteria:
1. Latest HbA1c reading between 42 and 46 mmol/mol (inclusive)
2. HbA1c reading must be within the last 12 months

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
HBA1CReadings AS (
    -- Get all HbA1c readings within last 12 months
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        RESULT_VALUE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'HBA1C_LEVEL'
        AND RESULT_VALUE > 0
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(year, -1, CURRENT_DATE())
),
LatestHBA1C AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        r.PERSON_ID,
        r.SK_PATIENT_ID,
        r.CLINICAL_EFFECTIVE_DATE AS LATEST_HBA1C_DATE,
        r.RESULT_VALUE AS LATEST_HBA1C_VALUE,
        (
            SELECT ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE)
            FROM HBA1CReadings h2
            WHERE h2.PERSON_ID = r.PERSON_ID
        ) AS ALL_HBA1C_CODES,
        (
            SELECT ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY)
            FROM HBA1CReadings h2
            WHERE h2.PERSON_ID = r.PERSON_ID
        ) AS ALL_HBA1C_DISPLAYS
    FROM HBA1CReadings r
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE 
        WHEN hba1c.LATEST_HBA1C_VALUE >= 42 AND hba1c.LATEST_HBA1C_VALUE <= 46 THEN TRUE
        ELSE FALSE
    END AS HAS_ELEVATED_HBA1C,
    hba1c.LATEST_HBA1C_DATE,
    hba1c.LATEST_HBA1C_VALUE,
    hba1c.ALL_HBA1C_CODES,
    hba1c.ALL_HBA1C_DISPLAYS
FROM BasePopulation bp
LEFT JOIN LatestHBA1C hba1c
    USING (PERSON_ID)
WHERE hba1c.LATEST_HBA1C_VALUE >= 42 
    AND hba1c.LATEST_HBA1C_VALUE <= 46; 