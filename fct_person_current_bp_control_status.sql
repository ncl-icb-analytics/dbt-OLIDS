CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_BP_CONTROL_STATUS (
    -- Identifiers
    PERSON_ID VARCHAR,
    SK_PATIENT_ID NUMBER(38,0),
    -- Latest BP Reading
    LATEST_BP_DATE DATE,
    LATEST_SYSTOLIC_VALUE NUMBER,
    LATEST_DIASTOLIC_VALUE NUMBER,
    -- Patient Characteristics used for logic
    AGE NUMBER,
    HAS_T2DM BOOLEAN,                      -- Derived from PERSON_CURRENT_DX_DIABETES
    HAS_CKD BOOLEAN,                       -- Derived from PERSON_CURRENT_DX_CKD (existence check)
    IS_DIAGNOSED_HTN BOOLEAN,              -- Derived from PERSON_CURRENT_DX_HYPERTENSION
    LATEST_ACR_VALUE NUMBER,               -- From PERSON_CURRENT_DX_CKD (used for CKD_ACR_GE_70 threshold)
    -- Applied Threshold Details
    APPLIED_THRESHOLD_RULE_ID VARCHAR,
    APPLIED_PATIENT_GROUP VARCHAR,
    APPLIED_SYSTOLIC_THRESHOLD NUMBER,
    APPLIED_DIASTOLIC_THRESHOLD NUMBER,
    -- BP Control Status relative to threshold
    IS_SYSTOLIC_CONTROLLED BOOLEAN,
    IS_DIASTOLIC_CONTROLLED BOOLEAN,
    IS_OVERALL_BP_CONTROLLED BOOLEAN,
    -- BP Reading Timeliness
    LATEST_BP_READING_AGE_MONTHS NUMBER,   -- Age of the latest BP reading in months
    IS_LATEST_BP_WITHIN_RECOMMENDED_INTERVAL BOOLEAN -- TRUE if latest BP is within guideline interval (updated logic)
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Calculates current Blood Pressure (BP) control status by applying patient-specific thresholds based on age, T2DM, CKD, and ACR level. Includes patient HTN diagnosis status. Selects highest priority threshold (< SBP/DBP) from RULESETS.BP_THRESHOLDS. Determines IS_OVERALL_BP_CONTROLLED and assesses timeliness (IS_LATEST_BP_WITHIN_RECOMMENDED_INTERVAL) using risk-group intervals (12 months for T2DM, CKD, or diagnosed HTN).'
AS
WITH LatestBP AS (
    -- Select the latest BP reading for each person
    SELECT PERSON_ID, CLINICAL_EFFECTIVE_DATE, SYSTOLIC_VALUE, DIASTOLIC_VALUE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST
),
PatientCharacteristics AS (
    -- Gather age, diabetes, CKD, AND Hypertension diagnosis status for each person with a BP reading
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
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_DIABETES dm ON bp.PERSON_ID = dm.PERSON_ID
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_CKD ckd ON bp.PERSON_ID = ckd.PERSON_ID -- Join to check existence
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_HYPERTENSION htn ON bp.PERSON_ID = htn.PERSON_ID
),
RankedThresholds AS (
    -- Join patient characteristics to applicable BP thresholds and rank them by priority
    SELECT
        pc.*, -- Includes HAS_CKD and IS_DIAGNOSED_HTN now
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
        ON (
            (thr.PATIENT_GROUP = 'AGE_LT_80' AND pc.AGE < 80) OR
            (thr.PATIENT_GROUP = 'AGE_GE_80' AND pc.AGE >= 80) OR
            -- Use IS_ON_DM_REGISTER and DIABETES_TYPE for T2DM check
            (thr.PATIENT_GROUP = 'T2DM' AND pc.IS_ON_DM_REGISTER AND pc.DIABETES_TYPE = 'Type 2' AND pc.AGE < 80) OR
            -- Use HAS_CKD (derived from join) for CKD check
            (thr.PATIENT_GROUP = 'CKD' AND pc.HAS_CKD AND pc.AGE < 80) OR
            -- Use HAS_CKD and LATEST_ACR_VALUE for specific CKD threshold
            (thr.PATIENT_GROUP = 'CKD_ACR_GE_70' AND pc.HAS_CKD AND pc.LATEST_ACR_VALUE >= 70 AND pc.AGE < 80)
           )
    WHERE thr.THRESHOLD_TYPE = 'TARGET_UPPER' AND thr.OPERATOR = 'BELOW'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pc.PERSON_ID ORDER BY priority_rank ASC) = 1
)
-- Final selection and calculation of control status including updated timeliness
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
    -- Applied threshold info
    rt.THRESHOLD_RULE_ID AS APPLIED_THRESHOLD_RULE_ID,
    rt.PATIENT_GROUP AS APPLIED_PATIENT_GROUP,
    rt.SYSTOLIC_THRESHOLD AS APPLIED_SYSTOLIC_THRESHOLD,
    rt.DIASTOLIC_THRESHOLD AS APPLIED_DIASTOLIC_THRESHOLD,
    -- Control Status Calculation
    (rt.LATEST_SYSTOLIC_VALUE IS NOT NULL AND rt.LATEST_SYSTOLIC_VALUE < rt.SYSTOLIC_THRESHOLD) AS IS_SYSTOLIC_CONTROLLED,
    (rt.LATEST_DIASTOLIC_VALUE IS NOT NULL AND rt.LATEST_DIASTOLIC_VALUE < rt.DIASTOLIC_THRESHOLD) AS IS_DIASTOLIC_CONTROLLED,
    (IS_SYSTOLIC_CONTROLLED AND IS_DIASTOLIC_CONTROLLED) AS IS_OVERALL_BP_CONTROLLED,
    -- Timeliness Calculation
    DATEDIFF(month, rt.LATEST_BP_DATE, CURRENT_DATE()) AS LATEST_BP_READING_AGE_MONTHS,
    -- ** UPDATED Timeliness Logic **
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
