CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_QRISK_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the latest QRISK score was recorded
    RESULT_VALUE NUMBER(6,2), -- The numeric result value of the latest QRISK score (6,2 format)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the QRISK observation
    CODE_DESCRIPTION VARCHAR, -- The textual description of the concept code
    QRISK_TYPE VARCHAR -- QRISK, QRISK2, or QRISK3 (derived from code description)
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing the latest QRISK cardiovascular risk score (QRISK, QRISK2, QRISK3) for each person. The QRISK algorithm estimates an individual\'s 10-year risk of developing cardiovascular disease based on a range of clinical and demographic factors. This table selects the most recent available QRISK score for each person, regardless of version, and includes the type of QRISK algorithm used. Note: QRISK scores are not calculated dynamically in this pipeline, but are taken as coded in the source systems.'
AS
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    RESULT_VALUE,
    CONCEPT_CODE,
    CODE_DESCRIPTION,
    QRISK_TYPE
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_QRISK_ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1; 