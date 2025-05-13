CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_COPD_SPIROMETRY (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the spirometry test
    RESULT_VALUE NUMBER, -- The actual FEV1/FVC ratio value
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either FEV1FVC_COD or FEV1FVCL70_COD
    IS_BELOW_0_7 BOOLEAN -- Flag indicating if the ratio is below 0.7
)
COMMENT = 'Intermediate table containing COPD spirometry results (FEV1/FVC ratios). Includes both raw FEV1/FVC values and pre-coded "less than 0.7" observations.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseSpirometryObservations AS (
    -- Fetches all spirometry-related observations
    -- Includes both raw FEV1/FVC ratios and pre-coded "less than 0.7" observations
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        O."result_value"::NUMBER AS RESULT_VALUE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('FEV1FVC_COD', 'FEV1FVCL70_COD')
)
-- Final selection with derived fields
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    OBSERVATION_ID,
    CLINICAL_EFFECTIVE_DATE,
    RESULT_VALUE,
    CONCEPT_CODE,
    CODE_DESCRIPTION,
    SOURCE_CLUSTER_ID,
    CASE
        WHEN SOURCE_CLUSTER_ID = 'FEV1FVCL70_COD' THEN TRUE -- Pre-coded as less than 0.7
        WHEN SOURCE_CLUSTER_ID = 'FEV1FVC_COD' AND RESULT_VALUE < 0.7 THEN TRUE -- Raw value less than 0.7
        ELSE FALSE
    END AS IS_BELOW_0_7
FROM BaseSpirometryObservations; 