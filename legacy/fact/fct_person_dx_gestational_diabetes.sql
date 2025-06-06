CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_GESTATIONAL_DIABETES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    IS_ON_GEST_DIAB_REGISTER BOOLEAN, -- Flag indicating if the person has a gestational diabetes diagnosis
    EARLIEST_GEST_DIAB_DIAGNOSIS_DATE DATE, -- Earliest diagnosis date for gestational diabetes
    LATEST_GEST_DIAB_DIAGNOSIS_DATE DATE, -- Latest diagnosis date for gestational diabetes
    ALL_GEST_DIAB_CONCEPT_CODES ARRAY, -- Array of all GESTDIAB_COD concept codes recorded for the person
    ALL_GEST_DIAB_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the GESTDIAB_COD concept codes
    IS_CURRENTLY_PREGNANT BOOLEAN, -- Flag indicating if the person is currently pregnant (from FCT_PERSON_PREGNANT)
    LATEST_PREG_COD_DATE DATE, -- Latest date of a pregnancy code (start of current pregnancy, if any)
    LATEST_PREGDEL_COD_DATE DATE -- Latest date of a pregnancy ended/delivery code (end of last pregnancy, if any)
)
COMMENT = 'Fact table identifying individuals with a diagnosis of gestational diabetes (using GESTDIAB_COD), including current pregnancy status from FCT_PERSON_PREGNANT.'
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
    WHERE MC.CLUSTER_ID = 'GESTDIAB_COD'
),
PersonLevelGestDiabAggregation AS (
    SELECT
        bo.PERSON_ID,
        ANY_VALUE(bo.SK_PATIENT_ID) as SK_PATIENT_ID,
        MIN(bo.CLINICAL_EFFECTIVE_DATE) AS EARLIEST_GEST_DIAB_DIAGNOSIS_DATE,
        MAX(bo.CLINICAL_EFFECTIVE_DATE) AS LATEST_GEST_DIAB_DIAGNOSIS_DATE,
        ARRAY_AGG(DISTINCT bo.CONCEPT_CODE) WITHIN GROUP (ORDER BY bo.CONCEPT_CODE) AS ALL_GEST_DIAB_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT bo.CODE_DESCRIPTION) WITHIN GROUP (ORDER BY bo.CODE_DESCRIPTION) AS ALL_GEST_DIAB_CONCEPT_DISPLAYS,
        TRUE AS IS_ON_GEST_DIAB_REGISTER
    FROM BaseObservations bo
    GROUP BY bo.PERSON_ID
)
SELECT
    pl.PERSON_ID,
    pl.SK_PATIENT_ID,
    pl.IS_ON_GEST_DIAB_REGISTER,
    pl.EARLIEST_GEST_DIAB_DIAGNOSIS_DATE,
    pl.LATEST_GEST_DIAB_DIAGNOSIS_DATE,
    pl.ALL_GEST_DIAB_CONCEPT_CODES,
    pl.ALL_GEST_DIAB_CONCEPT_DISPLAYS,
    preg.IS_CURRENTLY_PREGNANT,
    preg.LATEST_PREG_COD_DATE,
    preg.LATEST_PREGDEL_COD_DATE
FROM PersonLevelGestDiabAggregation pl
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_PREGNANT preg
    ON pl.PERSON_ID = preg.PERSON_ID; 