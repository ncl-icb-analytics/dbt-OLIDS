CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_NHS_HEALTH_CHECK_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the NHS Health Check was completed
    CONCEPT_CODE VARCHAR, -- SNOMED code for the NHS Health Check event
    CODE_DESCRIPTION VARCHAR -- Description of the NHS Health Check event
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded NHS Health Check completed events for all persons, based on a hardcoded list of SNOMED codes. Excludes records with NULL clinical effective date.'
AS
SELECT DISTINCT
    pp."person_id" AS person_id,
    p."sk_patient_id" AS sk_patient_id,
    o."clinical_effective_date"::DATE AS clinical_effective_date,
    c.concept_code,
    c.code_description
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS c
    ON o."observation_core_concept_id" = c.SOURCE_CODE_ID
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
WHERE c.CONCEPT_CODE IN (
    '1959151000006103',
    '1948791000006100',
    '1728781000006106',
    '523221000000100',
    '1728811000006108',
    '1728801000006105',
    '1728791000006109',
    '840391000000101',
    '840401000000103',
    '1053551000000105',
    '904471000000104',
    '904481000000102'
)
AND o."clinical_effective_date" IS NOT NULL; 