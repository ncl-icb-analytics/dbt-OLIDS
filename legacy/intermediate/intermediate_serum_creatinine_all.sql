CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SERUM_CREATININE_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the serum creatinine test was performed/recorded
    RESULT_VALUE NUMBER(6,1), -- The numeric result value of the serum creatinine test (float, 1 decimal place)
    RESULT_UNIT VARCHAR, -- The unit of measurement (typically µmol/L)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the serum creatinine observation
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded serum creatinine results for all persons. Filters based on CRE_COD concept codes. Excludes records with NULL result values. Only includes results in standard units (µmol/L).'
AS
-- Selects distinct serum creatinine observation records.
-- Uses DISTINCT as a precaution against potential duplicate source records.
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date"::DATE as clinical_effective_date,
    CAST(o."result_value" AS NUMBER(6,1)) as result_value,
    UNIT_CON."display" as result_unit,
    c.concept_code,
    c.code_description
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp ON o."patient_id" = pp."patient_id"
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p ON pp."patient_id" = p."id"
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS c ON o."observation_core_concept_id" = c.concept_id
LEFT JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS UNIT_CON ON o."result_value_unit_concept_id" = UNIT_CON."id"
WHERE c.CLUSTER_ID = 'CRE_COD'
    AND o."result_value" IS NOT NULL -- Exclude NULL results
    AND UNIT_CON."display" = 'µmol/L'; -- Only include results in standard units
