CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_66 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_STAGE_1_HYPERTENSION BOOLEAN, -- Flag indicating if person has stage 1 hypertension without risk factors
    LATEST_BP_DATE DATE, -- Date of most recent blood pressure reading
    LATEST_SYSTOLIC_BP NUMBER, -- Most recent systolic blood pressure value
    LATEST_DIASTOLIC_BP NUMBER, -- Most recent diastolic blood pressure value
    IS_CLINIC_BP BOOLEAN, -- Flag indicating if the latest reading is from clinic
    IS_HOME_BP BOOLEAN -- Flag indicating if the latest reading is from home/ambulatory
)
COMMENT = 'Dimension table for LTC LCS case finding indicator HTN_66: Patients with stage 1 hypertension who do NOT have additional risk factors:
1. Clinic BP: Systolic ≥ 140 mmHg or Diastolic ≥ 90 mmHg
2. Home/Ambulatory BP: Systolic ≥ 135 mmHg or Diastolic ≥ 85 mmHg

Exclusions:
- Patients on any LTC register
- Patients with NHS health check in last 24 months
- Patients with resolved hypertension
- Patients with white coat hypertension
- Patients with diabetes
- Patients on palliative care register
- Patients with severe hypertension (HTN_61)
- Patients with stage 2 hypertension (HTN_62)
- Patients with stage 2 hypertension who are BSA with risk factors (HTN_63)
- Patients with stage 1 hypertension who have risk factors (HTN_65)

Risk factors that would exclude from this indicator:
- Myocardial infarction, cerebral event, or claudication
- CKD (eGFR < 60)
- Diabetes
- BMI > 35
- Black or South Asian (BSA)'
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
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_HTN_BASE bp
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
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST bp
),
RiskFactors AS (
    -- Get patients with risk factors
    SELECT DISTINCT PERSON_ID
    FROM (
        -- Myocardial, cerebral and claudication
        SELECT DISTINCT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
        WHERE CLUSTER_ID IN ('HYPERTENSION_MYOCARDIAL', 'HYPERTENSION_CEREBRAL', 'HYPERTENSION_CLAUDICATION')

        UNION

        -- CKD (eGFR < 60)
        SELECT DISTINCT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
        WHERE CLUSTER_ID = 'HYPERTENSION_EGFR'
            AND RESULT_VALUE < 60

        UNION

        -- Diabetes
        SELECT DISTINCT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
        WHERE CLUSTER_ID = 'HYPERTENSION_DIABETES'

        UNION

        -- BMI > 35
        SELECT DISTINCT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
        WHERE CLUSTER_ID = 'HYPERTENSION_BMI'
            AND RESULT_VALUE > 35

        UNION

        -- Black or South Asian
        SELECT DISTINCT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
        WHERE CLUSTER_ID = 'HYPERTENSION_BSA'
    )
),
HigherPriorityPatients AS (
    -- Get patients from higher priority groups (HTN_61, HTN_62, HTN_63, and HTN_65)
    SELECT DISTINCT PERSON_ID
    FROM (
        SELECT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_61
        WHERE HAS_SEVERE_HYPERTENSION = TRUE

        UNION

        SELECT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_62
        WHERE HAS_STAGE_2_HYPERTENSION = TRUE

        UNION

        SELECT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_63
        WHERE HAS_STAGE_2_HYPERTENSION_BSA = TRUE

        UNION

        SELECT PERSON_ID
        FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_HTN_65
        WHERE HAS_STAGE_1_HYPERTENSION_RISK = TRUE
    )
)
-- Final selection
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    CASE
        WHEN (
            (bp_readings.IS_CLINIC_BP AND (
                bp_readings.LATEST_SYSTOLIC_BP >= 140 OR
                bp_readings.LATEST_DIASTOLIC_BP >= 90
            ))
            OR
            (bp_readings.IS_HOME_BP AND (
                bp_readings.LATEST_SYSTOLIC_BP >= 135 OR
                bp_readings.LATEST_DIASTOLIC_BP >= 85
            ))
        ) THEN TRUE
        ELSE FALSE
    END AS HAS_STAGE_1_HYPERTENSION,
    bp_readings.LATEST_BP_DATE,
    bp_readings.LATEST_SYSTOLIC_BP,
    bp_readings.LATEST_DIASTOLIC_BP,
    bp_readings.IS_CLINIC_BP,
    bp_readings.IS_HOME_BP
FROM BasePopulation bp
LEFT JOIN LatestBloodPressure bp_readings
    USING (PERSON_ID)
WHERE NOT EXISTS (
    SELECT 1 FROM HigherPriorityPatients hpp
    WHERE hpp.PERSON_ID = bp.PERSON_ID
)
AND NOT EXISTS (
    SELECT 1 FROM RiskFactors rf
    WHERE rf.PERSON_ID = bp.PERSON_ID
)
AND (
    -- Include patients with stage 1 hypertension
    (bp_readings.IS_CLINIC_BP AND (
        bp_readings.LATEST_SYSTOLIC_BP >= 140 OR
        bp_readings.LATEST_DIASTOLIC_BP >= 90
    ))
    OR
    (bp_readings.IS_HOME_BP AND (
        bp_readings.LATEST_SYSTOLIC_BP >= 135 OR
        bp_readings.LATEST_DIASTOLIC_BP >= 85
    ))
);
