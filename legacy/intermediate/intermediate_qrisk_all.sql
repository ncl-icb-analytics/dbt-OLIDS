CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_QRISK_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the QRISK score was recorded
    RESULT_VALUE NUMBER(6,2), -- The numeric result value of the QRISK score (6,2 format)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the QRISK observation
    CODE_DESCRIPTION VARCHAR, -- The textual description of the concept code
    QRISK_TYPE VARCHAR -- QRISK, QRISK2, or QRISK3 (derived from code description)
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded QRISK cardiovascular risk scores (QRISK, QRISK2, QRISK3) for all persons. The QRISK algorithm estimates an individual\'s 10-year risk of developing cardiovascular disease based on a range of clinical and demographic factors. This table includes all available QRISK scores, the type of QRISK algorithm used, and excludes records with NULL result values. Note: QRISK scores are not calculated dynamically in this pipeline, but are taken as coded in the source systems.'
AS
-- Selects distinct QRISK observation records.
SELECT DISTINCT
    pp."person_id" AS person_id,
    p."sk_patient_id" AS sk_patient_id,
    o."clinical_effective_date"::DATE AS clinical_effective_date,
    o."result_value" AS result_value,
    c.concept_code,
    c.code_description,
    SPLIT_PART(c.code_description, ' ', 1) AS qrisk_type -- Derive QRISK type
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS c
    ON o."observation_core_concept_id" = c.SOURCE_CODE_ID
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
WHERE c.CLUSTER_ID = 'QRISKSCORE_COD'
  AND o."result_value" IS NOT NULL;
