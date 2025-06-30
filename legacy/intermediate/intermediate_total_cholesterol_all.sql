CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_TOTAL_CHOLESTEROL_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the cholesterol test was performed/recorded
    RESULT_VALUE NUMBER(6,1), -- The numeric result value of the cholesterol test (float, 1 decimal place)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the cholesterol test observation
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded total cholesterol results for all persons. Filters based on CHOL2_COD cluster. Excludes records with NULL result values. Cholesterol values are stored as floats (1 decimal place).'
AS
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date"::DATE as clinical_effective_date,
    CAST(o."result_value" AS NUMBER(6,1)) as result_value,
    c.concept_code,
    c.code_description
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS c
    ON o."observation_core_concept_id" = c.SOURCE_CODE_ID
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
WHERE c.CLUSTER_ID = 'CHOL2_COD'
AND o."result_value" IS NOT NULL;
