CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DIABETES_8_CARE_PROCESSES(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    -- HbA1c
    LATEST_HBA1C_DATE DATE, -- Date of latest HbA1c test
    HBA1C_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if HbA1c was completed in last 12 months
    LATEST_HBA1C_VALUE NUMBER(6,1), -- Latest HbA1c result value
    -- Blood Pressure
    LATEST_BP_DATE DATE, -- Date of latest blood pressure reading
    BP_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if BP was completed in last 12 months
    -- Cholesterol
    LATEST_CHOLESTEROL_DATE DATE, -- Date of latest cholesterol test
    CHOLESTEROL_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if cholesterol was completed in last 12 months
    -- Serum Creatinine
    LATEST_CREATININE_DATE DATE, -- Date of latest serum creatinine test
    CREATININE_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if creatinine was completed in last 12 months
    -- Urine ACR
    LATEST_ACR_DATE DATE, -- Date of latest urine ACR test
    ACR_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if ACR was completed in last 12 months
    -- Foot Check
    LATEST_FOOT_CHECK_DATE DATE, -- Date of latest foot check
    FOOT_CHECK_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if foot check was completed in last 12 months
    -- BMI
    LATEST_BMI_DATE DATE, -- Date of latest BMI recording
    BMI_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if BMI was recorded in last 12 months
    -- Smoking
    LATEST_SMOKING_DATE DATE, -- Date of latest smoking status recording
    SMOKING_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if smoking status was recorded in last 12 months
    -- Overall Completion
    CARE_PROCESSES_COMPLETED NUMBER(1,0), -- Count of care processes completed in last 12 months (0-8)
    ALL_PROCESSES_COMPLETED BOOLEAN -- Flag indicating if all 8 care processes were completed in last 12 months
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Fact table tracking completion of the 8 diabetes care processes (HbA1c, Blood Pressure, Cholesterol, Serum Creatinine, Urine ACR, Foot Check, BMI, Smoking) for each person. Shows completion status within last 12 months.'
AS
WITH TwelveMonthsAgo AS (
    SELECT DATEADD(month, -12, CURRENT_DATE()) as twelve_months_ago
)
SELECT
    dr.PERSON_ID,
    dr.SK_PATIENT_ID,
    dr.
    -- HbA1c
    hba.CLINICAL_EFFECTIVE_DATE as LATEST_HBA1C_DATE,
    CASE WHEN hba.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as HBA1C_COMPLETED_IN_LAST_12M,
    hba.RESULT_VALUE as LATEST_HBA1C_VALUE,
    -- Blood Pressure
    bp.CLINICAL_EFFECTIVE_DATE as LATEST_BP_DATE,
    CASE WHEN bp.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as BP_COMPLETED_IN_LAST_12M,
    -- Cholesterol
    chol.CLINICAL_EFFECTIVE_DATE as LATEST_CHOLESTEROL_DATE,
    CASE WHEN chol.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as CHOLESTEROL_COMPLETED_IN_LAST_12M,
    -- Serum Creatinine
    cre.CLINICAL_EFFECTIVE_DATE as LATEST_CREATININE_DATE,
    CASE WHEN cre.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as CREATININE_COMPLETED_IN_LAST_12M,
    -- Urine ACR
    acr.CLINICAL_EFFECTIVE_DATE as LATEST_ACR_DATE,
    CASE WHEN acr.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as ACR_COMPLETED_IN_LAST_12M,
    -- Foot Check
    fc.CLINICAL_EFFECTIVE_DATE as LATEST_FOOT_CHECK_DATE,
    -- Check completed in last 12 months if:
    -- 1. We have a check date within 12 months AND
    -- 2. Either both feet were checked OR the unchecked foot is absent/amputated
    -- 3. The check wasn't declined or unsuitable
    CASE
        WHEN fc.CLINICAL_EFFECTIVE_DATE IS NOT NULL
            AND fc.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago
            AND (
                fc.BOTH_FEET_CHECKED
                OR (fc.LEFT_FOOT_CHECKED AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED))
                OR (fc.RIGHT_FOOT_CHECKED AND (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED))
            )
            AND NOT (fc.IS_UNSUITABLE OR fc.IS_DECLINED)
        THEN TRUE
        ELSE FALSE
    END as FOOT_CHECK_COMPLETED_IN_LAST_12M,
    -- BMI
    bmi.CLINICAL_EFFECTIVE_DATE as LATEST_BMI_DATE,
    CASE WHEN bmi.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as BMI_COMPLETED_IN_LAST_12M,
    -- Smoking
    smok.CLINICAL_EFFECTIVE_DATE as LATEST_SMOKING_DATE,
    CASE WHEN smok.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as SMOKING_COMPLETED_IN_LAST_12M,
    -- Calculate overall completion
    (CASE WHEN hba.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END +
     CASE WHEN bp.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END +
     CASE WHEN chol.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END +
     CASE WHEN cre.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END +
     CASE WHEN acr.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END +
     CASE
        WHEN fc.CLINICAL_EFFECTIVE_DATE IS NOT NULL
            AND fc.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago
            AND (
                fc.BOTH_FEET_CHECKED
                OR (fc.LEFT_FOOT_CHECKED AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED))
                OR (fc.RIGHT_FOOT_CHECKED AND (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED))
            )
            AND NOT (fc.IS_UNSUITABLE OR fc.IS_DECLINED)
        THEN 1 ELSE 0 END +
     CASE WHEN bmi.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END +
     CASE WHEN smok.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END
    ) as CARE_PROCESSES_COMPLETED,
    CASE
        WHEN (hba.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago AND
              bp.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago AND
              chol.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago AND
              cre.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago AND
              acr.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago AND
              (fc.CLINICAL_EFFECTIVE_DATE IS NOT NULL
                AND fc.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago
                AND (
                    fc.BOTH_FEET_CHECKED
                    OR (fc.LEFT_FOOT_CHECKED AND (fc.RIGHT_FOOT_ABSENT OR fc.RIGHT_FOOT_AMPUTATED))
                    OR (fc.RIGHT_FOOT_CHECKED AND (fc.LEFT_FOOT_ABSENT OR fc.LEFT_FOOT_AMPUTATED))
                )
                AND NOT (fc.IS_UNSUITABLE OR fc.IS_DECLINED)) AND
              bmi.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago AND
              smok.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago) THEN TRUE
        ELSE FALSE
    END as ALL_PROCESSES_COMPLETED
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_DIABETES dr
CROSS JOIN TwelveMonthsAgo t
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_HBA1C_LATEST hba
    ON dr.PERSON_ID = hba.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST bp
    ON dr.PERSON_ID = bp.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_TOTAL_CHOLESTEROL_LATEST chol
    ON dr.PERSON_ID = chol.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SERUM_CREATININE_LATEST cre
    ON dr.PERSON_ID = cre.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_LATEST acr
    ON dr.PERSON_ID = acr.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_FOOT_CHECK_LATEST fc
    ON dr.PERSON_ID = fc.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BMI_LATEST bmi
    ON dr.PERSON_ID = bmi.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_LATEST smok
    ON dr.PERSON_ID = smok.PERSON_ID;
