CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DIABETES_9_CARE_PROCESSES(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient

    -- HbA1c
    LATEST_HBA1C_DATE DATE, -- Date of latest HbA1c test
    HBA1C_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if HbA1c was completed in last 12 months

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

    -- Retinal Screening (9th process)
    LATEST_RETINAL_SCREENING_DATE DATE, -- Date of latest retinal screening
    RETINAL_SCREENING_COMPLETED_IN_LAST_12M BOOLEAN, -- Flag indicating if retinal screening was completed in last 12 months

    -- Overall Completion
    CARE_PROCESSES_8_COMPLETED NUMBER(1,0), -- Count of original 8 care processes completed in last 12 months (0-8)
    CARE_PROCESSES_9_COMPLETED NUMBER(1,0), -- Count of all 9 care processes completed in last 12 months (0-9)
    ALL_8_PROCESSES_COMPLETED BOOLEAN, -- Flag indicating if all original 8 care processes were completed in last 12 months
    ALL_9_PROCESSES_COMPLETED BOOLEAN -- Flag indicating if all 9 care processes were completed in last 12 months
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Fact table tracking completion of the 9 diabetes care processes (8 standard processes plus retinal screening). Shows completion status and dates within last 12 months for each process.'
AS
WITH TwelveMonthsAgo AS (
    SELECT DATEADD(month, -12, CURRENT_DATE()) as twelve_months_ago
)
SELECT
    eight.PERSON_ID,
    eight.SK_PATIENT_ID,

    -- Copy all fields from 8 processes table
    eight.LATEST_HBA1C_DATE,
    eight.HBA1C_COMPLETED_IN_LAST_12M,

    eight.LATEST_BP_DATE,
    eight.BP_COMPLETED_IN_LAST_12M,

    eight.LATEST_CHOLESTEROL_DATE,
    eight.CHOLESTEROL_COMPLETED_IN_LAST_12M,

    eight.LATEST_CREATININE_DATE,
    eight.CREATININE_COMPLETED_IN_LAST_12M,

    eight.LATEST_ACR_DATE,
    eight.ACR_COMPLETED_IN_LAST_12M,

    eight.LATEST_FOOT_CHECK_DATE,
    eight.FOOT_CHECK_COMPLETED_IN_LAST_12M,

    eight.LATEST_BMI_DATE,
    eight.BMI_COMPLETED_IN_LAST_12M,

    eight.LATEST_SMOKING_DATE,
    eight.SMOKING_COMPLETED_IN_LAST_12M,

    -- Add retinal screening
    ret.CLINICAL_EFFECTIVE_DATE as LATEST_RETINAL_SCREENING_DATE,
    CASE WHEN ret.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN TRUE ELSE FALSE END as RETINAL_SCREENING_COMPLETED_IN_LAST_12M,

    -- Overall completion
    eight.CARE_PROCESSES_COMPLETED as CARE_PROCESSES_8_COMPLETED,
    eight.CARE_PROCESSES_COMPLETED +
        CASE WHEN ret.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago THEN 1 ELSE 0 END as CARE_PROCESSES_9_COMPLETED,
    eight.ALL_PROCESSES_COMPLETED as ALL_8_PROCESSES_COMPLETED,
    CASE WHEN eight.ALL_PROCESSES_COMPLETED AND
              ret.CLINICAL_EFFECTIVE_DATE >= t.twelve_months_ago
         THEN TRUE ELSE FALSE
    END as ALL_9_PROCESSES_COMPLETED

FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DIABETES_8_CARE_PROCESSES eight
CROSS JOIN TwelveMonthsAgo t
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_RETINAL_SCREENING_LATEST ret
    ON eight.PERSON_ID = ret.PERSON_ID;
