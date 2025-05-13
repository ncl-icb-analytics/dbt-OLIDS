CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_BP_CONTROL_STATUS (
    -- Identifiers
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    -- Latest BP Reading Details
    LATEST_BP_DATE DATE, -- Date of the most recent blood pressure reading
    LATEST_SYSTOLIC_VALUE NUMBER, -- Systolic value of the latest BP reading
    LATEST_DIASTOLIC_VALUE NUMBER, -- Diastolic value of the latest BP reading
    -- Patient Characteristics used for determining BP thresholds and timeliness
    AGE NUMBER, -- Age of the person
    HAS_T2DM BOOLEAN, -- Flag: TRUE if the person has Type 2 Diabetes Mellitus
    HAS_CKD BOOLEAN, -- Flag: TRUE if the person has Chronic Kidney Disease
    IS_DIAGNOSED_HTN BOOLEAN, -- Flag: TRUE if the person has a current diagnosis of Hypertension
    LATEST_ACR_VALUE NUMBER, -- Latest Albumin-to-Creatinine Ratio value, relevant for CKD patients
    -- Applied Threshold Details from RULESETS.BP_THRESHOLDS
    APPLIED_THRESHOLD_RULE_ID VARCHAR, -- Identifier of the BP threshold rule applied to this person
    APPLIED_PATIENT_GROUP VARCHAR, -- Description of the patient group for the applied threshold (e.g., 'T2DM', 'CKD_ACR_GE_70')
    APPLIED_SYSTOLIC_THRESHOLD NUMBER, -- Systolic BP threshold value applied
    APPLIED_DIASTOLIC_THRESHOLD NUMBER, -- Diastolic BP threshold value applied
    -- BP Control Status relative to the applied threshold
    IS_SYSTOLIC_CONTROLLED BOOLEAN, -- Flag: TRUE if latest systolic value is below the applied systolic threshold
    IS_DIASTOLIC_CONTROLLED BOOLEAN, -- Flag: TRUE if latest diastolic value is below the applied diastolic threshold
    IS_OVERALL_BP_CONTROLLED BOOLEAN, -- Flag: TRUE if both systolic and diastolic BP are controlled
    -- BP Reading Timeliness Assessment
    LATEST_BP_READING_AGE_MONTHS NUMBER,   -- Age of the latest BP reading in months from the current date
    IS_LATEST_BP_WITHIN_RECOMMENDED_INTERVAL BOOLEAN -- Flag: TRUE if the latest BP reading is within the recommended interval based on risk factors (T2DM, CKD, HTN, Age)
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Calculates current Blood Pressure (BP) control status by applying patient-specific thresholds based on NICE NG136 including age, T2DM, CKD, and ACR level. Includes patient HTN diagnosis status. Selects highest priority threshold (< SBP/DBP) from RULESETS.BP_THRESHOLDS. Determines IS_OVERALL_BP_CONTROLLED and assesses timeliness (IS_LATEST_BP_WITHIN_RECOMMENDED_INTERVAL) using risk-group intervals (e.g., 12 months for T2DM, CKD, or diagnosed HTN).'
AS
WITH LatestBP AS (
    -- Selects the most recent blood pressure reading (systolic, diastolic, and date) for each person
    -- from the INTERMEDIATE_BLOOD_PRESSURE_LATEST table.
    SELECT PERSON_ID, CLINICAL_EFFECTIVE_DATE, SYSTOLIC_VALUE, DIASTOLIC_VALUE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST
),
PatientCharacteristics AS (
    -- Gathers key patient characteristics relevant for BP threshold determination and timeliness rules.
    -- This includes age, Type 2 Diabetes status, CKD status (and latest ACR value if CKD), and diagnosed Hypertension status.
    -- Starts with patients who have a BP reading and joins to relevant dimension/fact tables.
    SELECT
        bp.PERSON_ID,
        age.SK_PATIENT_ID,
        bp.CLINICAL_EFFECTIVE_DATE AS LATEST_BP_DATE,
        bp.SYSTOLIC_VALUE AS LATEST_SYSTOLIC_VALUE,
        bp.DIASTOLIC_VALUE AS LATEST_DIASTOLIC_VALUE,
        age.AGE,
        -- Diabetes Status
        COALESCE(dm.IS_ON_DM_REGISTER, FALSE) AS IS_ON_DM_REGISTER,
        dm.DIABETES_TYPE,
        (ckd.PERSON_ID IS NOT NULL) AS HAS_CKD,
        ckd.LATEST_ACR_VALUE,
        -- Hypertension Status
        COALESCE(htn.IS_ON_HTN_REGISTER_CALC, FALSE) AS IS_DIAGNOSED_HTN
    FROM LatestBP bp
    -- Essential joins
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age ON bp.PERSON_ID = age.PERSON_ID
    -- Left joins for comorbidities/status
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_DIABETES dm ON bp.PERSON_ID = dm.PERSON_ID
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_CKD ckd ON bp.PERSON_ID = ckd.PERSON_ID -- Join to check existence
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_HYPERTENSION htn ON bp.PERSON_ID = htn.PERSON_ID
),
RankedThresholds AS (
    -- Determines the most appropriate BP threshold for each patient based on their characteristics.
    -- Joins patient characteristics with the BP_THRESHOLDS ruleset.
    -- Assigns a priority to each applicable rule and selects the one with the highest priority (lowest rank number).
    -- Thresholds are filtered for TARGET_UPPER and OPERATOR = 'BELOW'.
    SELECT
        pc.*, -- Includes all fields from PatientCharacteristics
        thr.THRESHOLD_RULE_ID,
        thr.PATIENT_GROUP,
        thr.SYSTOLIC_THRESHOLD,
        thr.DIASTOLIC_THRESHOLD,
        CASE thr.PATIENT_GROUP
            WHEN 'CKD_ACR_GE_70' THEN 1
            WHEN 'T2DM'          THEN 2
            WHEN 'CKD'           THEN 3
            WHEN 'AGE_GE_80'     THEN 4
            WHEN 'AGE_LT_80'     THEN 5
            ELSE 99
        END AS priority_rank
    FROM PatientCharacteristics pc
    JOIN DATA_LAB_NCL_TRAINING_TEMP.RULESETS.BP_THRESHOLDS thr
        ON ( -- Defines conditions for matching patients to threshold rules based on their group criteria
            (thr.PATIENT_GROUP = 'AGE_LT_80' AND pc.AGE < 80) OR
            (thr.PATIENT_GROUP = 'AGE_GE_80' AND pc.AGE >= 80) OR
            -- T2DM patients under 80 have a specific threshold
            (thr.PATIENT_GROUP = 'T2DM' AND pc.IS_ON_DM_REGISTER AND pc.DIABETES_TYPE = 'Type 2' AND pc.AGE < 80) OR
            -- CKD patients under 80 (general CKD)
            (thr.PATIENT_GROUP = 'CKD' AND pc.HAS_CKD AND pc.AGE < 80) OR
            -- CKD patients under 80 with ACR >= 70 have a more stringent threshold
            (thr.PATIENT_GROUP = 'CKD_ACR_GE_70' AND pc.HAS_CKD AND pc.LATEST_ACR_VALUE >= 70 AND pc.AGE < 80)
           )
    WHERE thr.THRESHOLD_TYPE = 'TARGET_UPPER' AND thr.OPERATOR = 'BELOW' -- Considers only upper limit thresholds where BP should be below the value
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pc.PERSON_ID ORDER BY priority_rank ASC) = 1 -- Selects the highest priority rule per person
)
-- Final selection: Combines patient data, their latest BP reading, the applied threshold, and calculates control status and timeliness.
SELECT
    rt.PERSON_ID,
    rt.SK_PATIENT_ID,
    rt.LATEST_BP_DATE,
    rt.LATEST_SYSTOLIC_VALUE,
    rt.LATEST_DIASTOLIC_VALUE,
    rt.AGE,
    -- Final boolean flags for characteristics
    (rt.IS_ON_DM_REGISTER AND rt.DIABETES_TYPE = 'Type 2') AS HAS_T2DM,
    rt.HAS_CKD,
    rt.IS_DIAGNOSED_HTN, -- Output the HTN diagnosis flag
    rt.LATEST_ACR_VALUE,
    -- Details of the BP threshold rule that was applied to this patient
    rt.THRESHOLD_RULE_ID AS APPLIED_THRESHOLD_RULE_ID,
    rt.PATIENT_GROUP AS APPLIED_PATIENT_GROUP,
    rt.SYSTOLIC_THRESHOLD AS APPLIED_SYSTOLIC_THRESHOLD,
    rt.DIASTOLIC_THRESHOLD AS APPLIED_DIASTOLIC_THRESHOLD,
    -- Control Status Calculation
    (rt.LATEST_SYSTOLIC_VALUE IS NOT NULL AND rt.LATEST_SYSTOLIC_VALUE < rt.SYSTOLIC_THRESHOLD) AS IS_SYSTOLIC_CONTROLLED,
    (rt.LATEST_DIASTOLIC_VALUE IS NOT NULL AND rt.LATEST_DIASTOLIC_VALUE < rt.DIASTOLIC_THRESHOLD) AS IS_DIASTOLIC_CONTROLLED,
    (IS_SYSTOLIC_CONTROLLED AND IS_DIASTOLIC_CONTROLLED) AS IS_OVERALL_BP_CONTROLLED,
    -- Calculation of the age of the latest BP reading in months
    DATEDIFF(month, rt.LATEST_BP_DATE, CURRENT_DATE()) AS LATEST_BP_READING_AGE_MONTHS,
    -- Determines if the latest BP reading is within the recommended interval based on patient risk factors and age.
    -- Tiered logic: Higher risk (T2DM, CKD, HTN) requires more frequent checks (12 months).
    -- Lower risk, older (age 40+) requires checks every 24 months.
    -- Lower risk, younger (age < 40) requires checks every 60 months.
    CASE
        -- Tier 1: Needs check within 12 months if T2DM OR CKD OR Diagnosed HTN
        WHEN (HAS_T2DM OR rt.HAS_CKD OR rt.IS_DIAGNOSED_HTN)
            THEN (LATEST_BP_READING_AGE_MONTHS <= 12)
        -- Tier 2: No T2DM/CKD/HTN, Age >= 40 -> Needs check within 24 months
        WHEN (NOT HAS_T2DM AND NOT rt.HAS_CKD AND NOT rt.IS_DIAGNOSED_HTN AND rt.AGE >= 40)
            THEN (LATEST_BP_READING_AGE_MONTHS <= 24)
         -- Tier 3: No T2DM/CKD/HTN, Age < 40 -> Needs check within 60 months
        WHEN (NOT HAS_T2DM AND NOT rt.HAS_CKD AND NOT rt.IS_DIAGNOSED_HTN AND rt.AGE < 40)
            THEN (LATEST_BP_READING_AGE_MONTHS <= 60)
        ELSE FALSE
    END AS IS_LATEST_BP_WITHIN_RECOMMENDED_INTERVAL
FROM RankedThresholds rt;
