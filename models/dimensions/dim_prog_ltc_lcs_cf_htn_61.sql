CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_61 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_SEVERE_HYPERTENSION BOOLEAN, -- Flag indicating if person has severe hypertension
    LATEST_BP_DATE DATE, -- Date of most recent blood pressure reading
    LATEST_SYSTOLIC_BP NUMBER, -- Most recent systolic blood pressure value
    LATEST_DIASTOLIC_BP NUMBER, -- Most recent diastolic blood pressure value
    IS_CLINIC_BP BOOLEAN, -- Flag indicating if the latest reading is from clinic
    IS_HOME_BP BOOLEAN -- Flag indicating if the latest reading is from home/ambulatory
)
COMMENT = 'Dimension table for LTC LCS case finding indicator HTN_61: Patients with severe hypertension based on blood pressure readings:
1. Clinic BP: Systolic ≥ 180 mmHg or Diastolic ≥ 120 mmHg
2. Home/Ambulatory BP: Systolic ≥ 170 mmHg or Diastolic ≥ 115 mmHg

Exclusions:
- Patients on any LTC register
- Patients with NHS health check in last 24 months
- Patients with resolved hypertension
- Patients with white coat hypertension
- Patients with diabetes
- Patients on palliative care register'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population from intermediate table
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        bp.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_HTN_BASE bp
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
    CASE 
        WHEN bp_readings.IS_CLINIC_BP AND (
            bp_readings.LATEST_SYSTOLIC_BP >= 180 OR 
            bp_readings.LATEST_DIASTOLIC_BP >= 120
        ) THEN TRUE
        WHEN bp_readings.IS_HOME_BP AND (
            bp_readings.LATEST_SYSTOLIC_BP >= 170 OR 
            bp_readings.LATEST_DIASTOLIC_BP >= 115
        ) THEN TRUE
        ELSE FALSE
    END AS HAS_SEVERE_HYPERTENSION,
    bp_readings.LATEST_BP_DATE,
    bp_readings.LATEST_SYSTOLIC_BP,
    bp_readings.LATEST_DIASTOLIC_BP,
    bp_readings.IS_CLINIC_BP,
    bp_readings.IS_HOME_BP
FROM BasePopulation bp
LEFT JOIN LatestBloodPressure bp_readings
    USING (PERSON_ID)
WHERE (
    -- Include patients with severe hypertension
    (bp_readings.IS_CLINIC_BP AND (
        bp_readings.LATEST_SYSTOLIC_BP >= 180 OR 
        bp_readings.LATEST_DIASTOLIC_BP >= 120
    ))
    OR 
    (bp_readings.IS_HOME_BP AND (
        bp_readings.LATEST_SYSTOLIC_BP >= 170 OR 
        bp_readings.LATEST_DIASTOLIC_BP >= 115
    ))
); 