CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_DM_61 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_DIABETES_RISK BOOLEAN, -- Flag indicating if person meets any diabetes risk criteria
    LATEST_HBA1C_DATE DATE, -- Date of most recent HbA1c reading
    LATEST_HBA1C_VALUE NUMBER, -- Most recent HbA1c value
    LATEST_QDIABETES_DATE DATE, -- Date of most recent QDiabetes score
    LATEST_QDIABETES_VALUE NUMBER, -- Most recent QDiabetes score
    LATEST_QRISK_DATE DATE, -- Date of most recent QRisk score
    LATEST_QRISK_VALUE NUMBER, -- Most recent QRisk score
    HAS_GESTATIONAL_DIABETES BOOLEAN, -- Flag indicating history of gestational diabetes
    ALL_HBA1C_CODES ARRAY, -- Array of all HbA1c codes
    ALL_HBA1C_DISPLAYS ARRAY -- Array of all HbA1c display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator DM_61: Patients at risk of diabetes who meet ANY of the following criteria:
1. HbA1c ≥ 42 mmol/mol within the last 5 years
2. QDiabetes score ≥ 5.6%
3. QRisk2 score > 20%
4. History of gestational diabetes

Exclusions:
- Patients on any LTC register
- Patients with NHS health check in last 24 months
- Patients under 17 years of age

Only includes patients who meet at least one inclusion criterion and no exclusion criteria.'
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
    -- Get all HbA1c readings with values > 0 within last 5 years
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
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(year, -5, CURRENT_DATE())
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
),
QDiabetesScores AS (
    -- Get the most recent QDiabetes score for each person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE AS LATEST_QDIABETES_DATE,
        RESULT_VALUE AS LATEST_QDIABETES_VALUE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'QDIABETES_RISK'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
),
QRiskScores AS (
    -- Get the most recent QRisk score for each person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE AS LATEST_QRISK_DATE,
        RESULT_VALUE AS LATEST_QRISK_VALUE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'QRISK2_10YEAR'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
),
GestationalDiabetes AS (
    -- Get patients with history of gestational diabetes
    SELECT DISTINCT
        PERSON_ID,
        SK_PATIENT_ID,
        TRUE AS HAS_GESTATIONAL_DIABETES
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'HISTORY_GESTATIONAL_DIABETES'
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE
        WHEN hba1c.LATEST_HBA1C_VALUE >= 42 OR
             qd.LATEST_QDIABETES_VALUE >= 5.6 OR
             qr.LATEST_QRISK_VALUE > 20 OR
             gd.HAS_GESTATIONAL_DIABETES = TRUE THEN TRUE
        ELSE FALSE
    END AS HAS_DIABETES_RISK,
    hba1c.LATEST_HBA1C_DATE,
    hba1c.LATEST_HBA1C_VALUE,
    qd.LATEST_QDIABETES_DATE,
    qd.LATEST_QDIABETES_VALUE,
    qr.LATEST_QRISK_DATE,
    qr.LATEST_QRISK_VALUE,
    COALESCE(gd.HAS_GESTATIONAL_DIABETES, FALSE) AS HAS_GESTATIONAL_DIABETES,
    hba1c.ALL_HBA1C_CODES,
    hba1c.ALL_HBA1C_DISPLAYS
FROM BasePopulation bp
LEFT JOIN LatestHBA1C hba1c
    USING (PERSON_ID)
LEFT JOIN QDiabetesScores qd
    USING (PERSON_ID)
LEFT JOIN QRiskScores qr
    USING (PERSON_ID)
LEFT JOIN GestationalDiabetes gd
    USING (PERSON_ID)
WHERE hba1c.LATEST_HBA1C_VALUE >= 42
    OR qd.LATEST_QDIABETES_VALUE >= 5.6
    OR qr.LATEST_QRISK_VALUE > 20
    OR gd.HAS_GESTATIONAL_DIABETES = TRUE;
