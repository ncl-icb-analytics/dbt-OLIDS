CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_62 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_STAGE_2_HYPERTENSION BOOLEAN, -- Flag indicating if person has stage 2 hypertension
    LATEST_BP_DATE DATE, -- Date of most recent blood pressure reading
    LATEST_SYSTOLIC_BP NUMBER, -- Most recent systolic blood pressure value
    LATEST_DIASTOLIC_BP NUMBER, -- Most recent diastolic blood pressure value
    IS_CLINIC_BP BOOLEAN, -- Flag indicating if the latest reading is from clinic
    IS_HOME_BP BOOLEAN -- Flag indicating if the latest reading is from home/ambulatory
)
COMMENT = 'Dimension table for LTC LCS case finding indicator HTN_62: Patients with stage 2 hypertension based on blood pressure readings:
1. Clinic BP: Systolic ≥ 160 mmHg or Diastolic ≥ 100 mmHg
2. Home/Ambulatory BP: Systolic ≥ 150 mmHg or Diastolic ≥ 95 mmHg

Exclusions:
- Patients on any LTC register
- Patients with NHS health check in last 24 months
- Patients with resolved hypertension
- Patients with white coat hypertension
- Patients with diabetes
- Patients on palliative care register
- Patients with severe hypertension (HTN_61)'
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
),
SevereHypertension AS (
    -- Get patients with severe hypertension (HTN_61) to exclude them
    SELECT DISTINCT PERSON_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_61
    WHERE HAS_SEVERE_HYPERTENSION = TRUE
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE
        WHEN bp_readings.IS_CLINIC_BP AND (
            bp_readings.LATEST_SYSTOLIC_BP >= 160 OR
            bp_readings.LATEST_DIASTOLIC_BP >= 100
        ) THEN TRUE
        WHEN bp_readings.IS_HOME_BP AND (
            bp_readings.LATEST_SYSTOLIC_BP >= 150 OR
            bp_readings.LATEST_DIASTOLIC_BP >= 95
        ) THEN TRUE
        ELSE FALSE
    END AS HAS_STAGE_2_HYPERTENSION,
    bp_readings.LATEST_BP_DATE,
    bp_readings.LATEST_SYSTOLIC_BP,
    bp_readings.LATEST_DIASTOLIC_BP,
    bp_readings.IS_CLINIC_BP,
    bp_readings.IS_HOME_BP
FROM BasePopulation bp
LEFT JOIN LatestBloodPressure bp_readings
    USING (PERSON_ID)
WHERE NOT EXISTS (
    SELECT 1 FROM SevereHypertension sh
    WHERE sh.PERSON_ID = bp.PERSON_ID
)
AND (
    -- Include patients with stage 2 hypertension
    (bp_readings.IS_CLINIC_BP AND (
        bp_readings.LATEST_SYSTOLIC_BP >= 160 OR
        bp_readings.LATEST_DIASTOLIC_BP >= 100
    ))
    OR
    (bp_readings.IS_HOME_BP AND (
        bp_readings.LATEST_SYSTOLIC_BP >= 150 OR
        bp_readings.LATEST_DIASTOLIC_BP >= 95
    ))
);
