CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_HBA1C_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest HbA1c test
    RESULT_VALUE NUMBER(6,1), -- The numeric result value of the HbA1c test (float, up to 2 decimal places)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the HbA1c test observation
    CODE_DESCRIPTION VARCHAR, -- The textual description of the concept code
    IS_IFCC BOOLEAN, -- Flag indicating if this is an IFCC measurement
    IS_DCCT BOOLEAN -- Flag indicating if this is a DCCT measurement
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing the latest HbA1c result for each person, with flags for IFCC and DCCT measurement types. HbA1c values are stored as floats.'
AS
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    RESULT_VALUE,
    CONCEPT_CODE,
    CODE_DESCRIPTION,
    IS_IFCC,
    IS_DCCT
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC, RESULT_VALUE DESC) AS RN
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_HBA1C_ALL
)
WHERE RN = 1; 