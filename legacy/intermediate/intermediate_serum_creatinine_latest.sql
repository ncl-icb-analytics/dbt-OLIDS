CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SERUM_CREATININE_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the latest serum creatinine test was performed/recorded
    RESULT_VALUE NUMBER(6,1), -- The numeric result value of the latest serum creatinine test (float, 1 decimal place)
    RESULT_UNIT VARCHAR, -- The unit of measurement (typically µmol/L)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the serum creatinine observation
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing the latest serum creatinine result for each person. Derived from INTERMEDIATE_SERUM_CREATININE_ALL.'
AS
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    RESULT_VALUE,
    RESULT_UNIT,
    CONCEPT_CODE,
    CODE_DESCRIPTION
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SERUM_CREATININE_ALL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY PERSON_ID
    ORDER BY CLINICAL_EFFECTIVE_DATE DESC, RESULT_VALUE DESC
) = 1;
