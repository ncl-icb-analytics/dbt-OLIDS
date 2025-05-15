CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_FHYP (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    IS_ON_FHYP_REGISTER BOOLEAN, -- Flag indicating if the person is on the familial hypercholesterolaemia register
    EARLIEST_FHYP_DIAGNOSIS_DATE DATE, -- Earliest diagnosis date for familial hypercholesterolaemia
    LATEST_FHYP_DIAGNOSIS_DATE DATE, -- Latest diagnosis date for familial hypercholesterolaemia
    ALL_FHYP_CONCEPT_CODES ARRAY, -- Array of all FHYP_COD concept codes recorded for the person
    ALL_FHYP_CONCEPT_DISPLAYS ARRAY -- Array of display terms for the FHYP_COD concept codes
)
COMMENT = 'Fact table identifying individuals with a diagnosis of familial hypercholesterolaemia (FH), using FHYP_COD cluster. No resolved codes are used.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseObservations AS (
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID = 'FHYP_COD'
),
PersonLevelFHYPAggregation AS (
    SELECT
        bo.PERSON_ID,
        ANY_VALUE(bo.SK_PATIENT_ID) as SK_PATIENT_ID,
        MIN(bo.CLINICAL_EFFECTIVE_DATE) AS EARLIEST_FHYP_DIAGNOSIS_DATE,
        MAX(bo.CLINICAL_EFFECTIVE_DATE) AS LATEST_FHYP_DIAGNOSIS_DATE,
        ARRAY_AGG(DISTINCT bo.CONCEPT_CODE) WITHIN GROUP (ORDER BY bo.CONCEPT_CODE) AS ALL_FHYP_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT bo.CODE_DESCRIPTION) WITHIN GROUP (ORDER BY bo.CODE_DESCRIPTION) AS ALL_FHYP_CONCEPT_DISPLAYS,
        TRUE AS IS_ON_FHYP_REGISTER
    FROM BaseObservations bo
    GROUP BY bo.PERSON_ID
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    IS_ON_FHYP_REGISTER,
    EARLIEST_FHYP_DIAGNOSIS_DATE,
    LATEST_FHYP_DIAGNOSIS_DATE,
    ALL_FHYP_CONCEPT_CODES,
    ALL_FHYP_CONCEPT_DISPLAYS
FROM PersonLevelFHYPAggregation; 