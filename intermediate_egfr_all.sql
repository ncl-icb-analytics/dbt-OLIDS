CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_EGFR_ALL (
    person_id,
    sk_patient_id,
    clinical_effective_date,
    result_value,
    CONCEPT_CODE,
    CODE_DESCRIPTION,
    OBSERVATION_ID
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Select distinct rows to avoid duplicates.
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date" as clinical_effective_date,
    o."result_value" as result_value,
    c.concept_code,
    c.code_description, -- Using the description from MAPPED_CONCEPTS
    o."id" as observation_id
-- Source table for observations.
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION O
-- Join to the pre-aggregated MAPPED_CONCEPTS table to get cluster info and codes
JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS C
    ON O."observation_core_concept_id" = C.SOURCE_CODE_ID
-- Join to get person identifier
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
-- Join to get patient surrogate key
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
-- Filter for the specific eGFR cluster ID.
WHERE C.CLUSTER_ID = 'EGFR_COD'
-- Filter out records where the result value is missing.
AND o."result_value" IS NOT NULL;



