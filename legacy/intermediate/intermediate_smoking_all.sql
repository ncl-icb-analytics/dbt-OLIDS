CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either SMOK_COD (general smoking codes), LSMOK_COD (current smoker codes), EXSMOK_COD (ex-smoker codes), or NSMOK_COD (never smoked codes)
    IS_SMOKER_CODE BOOLEAN, -- Flag indicating if this is a current smoker code (LSMOK_COD)
    IS_EX_SMOKER_CODE BOOLEAN, -- Flag indicating if this is an ex-smoker code (EXSMOK_COD)
    IS_NEVER_SMOKED_CODE BOOLEAN -- Flag indicating if this is a never smoked code (NSMOK_COD)
)
COMMENT = 'Intermediate table containing all smoking status observations using QOF definitions. Collects raw smoking codes chronologically. Uses cluster IDs: SMOK_COD (general smoking codes), LSMOK_COD (current smoker codes), EXSMOK_COD (ex-smoker codes), and NSMOK_COD (never smoked codes). The fact table FCT_PERSON_SMOKING_STATUS determines the latest smoking status based on these codes.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT
    O."id" AS OBSERVATION_ID,
    PP."person_id" AS PERSON_ID,
    P."sk_patient_id" AS SK_PATIENT_ID,
    O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
    MC.CONCEPT_CODE,
    MC.CODE_DESCRIPTION,
    MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
    -- Flag different types of smoking codes
    CASE WHEN MC.CLUSTER_ID = 'LSMOK_COD' THEN TRUE ELSE FALSE END AS IS_SMOKER_CODE,
    CASE WHEN MC.CLUSTER_ID = 'EXSMOK_COD' THEN TRUE ELSE FALSE END AS IS_EX_SMOKER_CODE,
    CASE WHEN MC.CLUSTER_ID = 'NSMOK_COD' THEN TRUE ELSE FALSE END AS IS_NEVER_SMOKED_CODE
FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
    ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
    ON O."patient_id" = PP."patient_id"
JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
    ON O."patient_id" = P."id"
WHERE MC.CLUSTER_ID IN ('SMOK_COD', 'LSMOK_COD', 'EXSMOK_COD', 'NSMOK_COD');
