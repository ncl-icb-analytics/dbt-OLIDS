CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_TOTAL_CHOLESTEROL_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest cholesterol test
    RESULT_VALUE NUMBER(6,1), -- The numeric result value of the cholesterol test (float, 1 decimal place)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the cholesterol test observation
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing the latest total cholesterol result for each person, based on the most recent clinical effective date. Cholesterol values are stored as floats (1 decimal place).'
AS
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    RESULT_VALUE,
    CONCEPT_CODE,
    CODE_DESCRIPTION
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC, RESULT_VALUE DESC) AS RN
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_TOTAL_CHOLESTEROL_ALL
)
WHERE RN = 1; 