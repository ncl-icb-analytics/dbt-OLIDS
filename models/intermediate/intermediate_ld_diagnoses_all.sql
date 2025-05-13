CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LD_DIAGNOSES_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the learning disability diagnosis was recorded
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the learning disability diagnosis
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Intermediate table containing all recorded learning disability diagnoses for all persons. Filters based on the LD_DIAGNOSIS_COD cluster ID from MAPPED_CONCEPTS.'
AS
-- Selects distinct learning disability diagnosis records.
-- Uses DISTINCT as a precaution against potential duplicate source records.
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date"::DATE as clinical_effective_date, -- Cast to DATE
    c.concept_code,
    c.code_description -- Using the description from MAPPED_CONCEPTS
-- Source table for observations.
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION O
-- Join to MAPPED_CONCEPTS to filter based on the cluster ID and get code details.
JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS C
    ON O."observation_core_concept_id" = C.SOURCE_CODE_ID
-- Join to link observation patient_id to person_id.
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
-- Join to link observation patient_id to patient surrogate key.
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
-- Filter for observations belonging to the learning disability diagnosis code cluster.
WHERE C.CLUSTER_ID = 'LD_DIAGNOSIS_COD'; 