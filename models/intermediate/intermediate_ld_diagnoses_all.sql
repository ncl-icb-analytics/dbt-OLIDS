CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LD_DIAGNOSES_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person at the time of the latest refresh
    OBSERVATION_ID VARCHAR, -- Identifier for the specific learning disability observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the learning disability diagnosis was recorded
    CONCEPT_CODE VARCHAR, -- The specific concept code for the learning disability
    CONCEPT_DISPLAY VARCHAR, -- The display term for the learning disability concept code
    SOURCE_CLUSTER_ID VARCHAR -- The cluster ID, will be 'LD_COD' for these records
)
COMMENT = 'Intermediate table holding all raw learning disability (LD_COD from PCD source) observations, irrespective of age. Includes person demographics like age for downstream filtering.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Selects all learning disability observations (LD_COD from PCD source)
-- and joins with patient and age dimensions to include person-level details.
SELECT
    PP."person_id" AS PERSON_ID,
    PAT."sk_patient_id" AS SK_PATIENT_ID,
    age_dim.AGE,
    O."id" AS OBSERVATION_ID,
    O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
    MC.CONCEPT_CODE,
    MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY,
    MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
    ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
    ON O."patient_id" = PP."patient_id"
JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS PAT
    ON PP."patient_id" = PAT."id"
JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age_dim
    ON PP."person_id" = age_dim.PERSON_ID
WHERE 
    MC.CLUSTER_ID = 'LD_COD' AND MC.SOURCE = 'PCD'; 