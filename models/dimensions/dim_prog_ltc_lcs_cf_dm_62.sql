CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_62 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_GESTATIONAL_DIABETES_RISK BOOLEAN, -- Flag indicating if person has gestational diabetes and pregnancy risk
    LATEST_HBA1C_DATE DATE, -- Date of most recent HbA1c reading (if any)
    LATEST_HBA1C_VALUE NUMBER, -- Most recent HbA1c value (if any)
    ALL_GESTATIONAL_DIABETES_CODES ARRAY, -- Array of all gestational diabetes codes
    ALL_GESTATIONAL_DIABETES_DISPLAYS ARRAY -- Array of all gestational diabetes display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator DM_62: Patients with gestational diabetes and pregnancy risk who meet ALL of the following criteria:
1. Has gestational diabetes and pregnancy risk diagnosis
2. No HbA1c reading in the last 12 months

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
GestationalDiabetesRisk AS (
    -- Get patients with gestational diabetes and pregnancy risk
    SELECT DISTINCT
        PERSON_ID,
        SK_PATIENT_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_GESTATIONAL_DIABETES_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_GESTATIONAL_DIABETES_DISPLAYS
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'GESTATIONAL_DIABETES_PREGNANCY_RISK'
    GROUP BY PERSON_ID, SK_PATIENT_ID
),
LatestHBA1C AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE AS LATEST_HBA1C_DATE,
        RESULT_VALUE AS LATEST_HBA1C_VALUE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'HBA1C_LEVEL'
        AND RESULT_VALUE > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE 
        WHEN gd.PERSON_ID IS NOT NULL AND 
             (hba1c.LATEST_HBA1C_DATE IS NULL OR 
              hba1c.LATEST_HBA1C_DATE < DATEADD(year, -1, CURRENT_DATE())) THEN TRUE
        ELSE FALSE
    END AS HAS_GESTATIONAL_DIABETES_RISK,
    hba1c.LATEST_HBA1C_DATE,
    hba1c.LATEST_HBA1C_VALUE,
    gd.ALL_GESTATIONAL_DIABETES_CODES,
    gd.ALL_GESTATIONAL_DIABETES_DISPLAYS
FROM BasePopulation bp
LEFT JOIN GestationalDiabetesRisk gd
    USING (PERSON_ID)
LEFT JOIN LatestHBA1C hba1c
    USING (PERSON_ID)
WHERE gd.PERSON_ID IS NOT NULL 
    AND (hba1c.LATEST_HBA1C_DATE IS NULL OR 
         hba1c.LATEST_HBA1C_DATE < DATEADD(year, -1, CURRENT_DATE())); 