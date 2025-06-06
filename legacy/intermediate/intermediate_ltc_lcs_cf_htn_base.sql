CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_HTN_BASE (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_WHITE_COAT_HYPERTENSION BOOLEAN, -- Flag indicating if person has white coat hypertension
    HAS_RESOLVED_HYPERTENSION BOOLEAN, -- Flag indicating if person has resolved hypertension
    LATEST_BP_DATE DATE, -- Date of most recent blood pressure reading
    LATEST_SYSTOLIC_BP NUMBER, -- Most recent systolic blood pressure
    LATEST_DIASTOLIC_BP NUMBER, -- Most recent diastolic blood pressure
    IS_CLINIC_BP BOOLEAN, -- Flag indicating if the latest reading is from clinic
    IS_HOME_BP BOOLEAN -- Flag indicating if the latest reading is from home/ambulatory
)
COMMENT = 'Intermediate table for LTC LCS case finding hypertension base population. 
Excludes patients with:
- White coat hypertension
- Resolved hypertension
- Diabetes
- Palliative care

This base population is used by all hypertension indicators (61-66) as their starting point.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients
    -- Note: Base population already excludes those on LTC registers and with NHS health check in last 24 months
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
),
Exclusions AS (
    -- Get patients with white coat hypertension, resolved hypertension, diabetes, or palliative care
    SELECT DISTINCT
        PERSON_ID,
        BOOLOR_AGG(CASE WHEN CLUSTER_ID = 'HYPERTENSION_WHITE_COAT' THEN TRUE ELSE FALSE END) AS HAS_WHITE_COAT_HYPERTENSION,
        BOOLOR_AGG(CASE WHEN CLUSTER_ID = 'HYPERTENSION_RESOLVED' THEN TRUE ELSE FALSE END) AS HAS_RESOLVED_HYPERTENSION
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID IN ('HYPERTENSION_WHITE_COAT', 'HYPERTENSION_RESOLVED', 'TYPE_2_DIABETES', 'PALLIATIVE_CARE')
    GROUP BY PERSON_ID
),
LatestBloodPressure AS (
    -- Get the most recent blood pressure reading for each person
    SELECT
        bp.PERSON_ID,
        bp.CLINICAL_EFFECTIVE_DATE AS LATEST_BP_DATE,
        bp.SYSTOLIC_VALUE AS LATEST_SYSTOLIC_BP,
        bp.DIASTOLIC_VALUE AS LATEST_DIASTOLIC_BP,
        -- If not home or ABPM, assume it's clinic BP
        NOT (bp.IS_HOME_BP_EVENT OR bp.IS_ABPM_BP_EVENT) AS IS_CLINIC_BP,
        bp.IS_HOME_BP_EVENT OR bp.IS_ABPM_BP_EVENT AS IS_HOME_BP
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST bp
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    COALESCE(ex.HAS_WHITE_COAT_HYPERTENSION, FALSE) AS HAS_WHITE_COAT_HYPERTENSION,
    COALESCE(ex.HAS_RESOLVED_HYPERTENSION, FALSE) AS HAS_RESOLVED_HYPERTENSION,
    bp_readings.LATEST_BP_DATE,
    bp_readings.LATEST_SYSTOLIC_BP,
    bp_readings.LATEST_DIASTOLIC_BP,
    bp_readings.IS_CLINIC_BP,
    bp_readings.IS_HOME_BP
FROM BasePopulation bp
LEFT JOIN Exclusions ex
    USING (PERSON_ID)
LEFT JOIN LatestBloodPressure bp_readings
    USING (PERSON_ID)
WHERE NOT COALESCE(ex.HAS_WHITE_COAT_HYPERTENSION, FALSE)
    AND NOT COALESCE(ex.HAS_RESOLVED_HYPERTENSION, FALSE)
    AND NOT EXISTS (
        SELECT 1 
        FROM Exclusions e2 
        WHERE e2.PERSON_ID = bp.PERSON_ID 
        AND e2.HAS_RESOLVED_HYPERTENSION IS NULL  -- This means they have diabetes or palliative care
    ); 