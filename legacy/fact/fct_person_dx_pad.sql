CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_PAD (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_PAD_REGISTER BOOLEAN, -- Flag indicating if person is on the PAD register (always TRUE for rows in this table)
    EARLIEST_PAD_DATE DATE, -- Earliest PAD diagnosis date
    LATEST_PAD_DATE DATE, -- Latest PAD diagnosis date
    ALL_PAD_CONCEPT_CODES ARRAY, -- All PAD concept codes
    ALL_PAD_CONCEPT_DISPLAYS ARRAY -- All PAD concept display terms
)
COMMENT = 'Fact table identifying individuals with a PAD diagnosis code. Simply includes all patients with a PAD_COD code, with no additional filters.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all PAD diagnoses
    SELECT 
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        AGE.AGE,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE AS AGE
        ON PP."person_id" = AGE.PERSON_ID
    WHERE MC.CLUSTER_ID = 'PAD_COD'
),
PersonLevelAggregation AS (
    -- Aggregate to one row per person with earliest/latest dates and concept arrays
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        AGE,
        MIN(CLINICAL_EFFECTIVE_DATE) AS EARLIEST_PAD_DATE,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_PAD_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_PAD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_PAD_CONCEPT_DISPLAYS
    FROM BaseObservations
    GROUP BY PERSON_ID, SK_PATIENT_ID, AGE
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    TRUE AS IS_ON_PAD_REGISTER, -- All patients in this table are on the register
    EARLIEST_PAD_DATE,
    LATEST_PAD_DATE,
    ALL_PAD_CONCEPT_CODES,
    ALL_PAD_CONCEPT_DISPLAYS
FROM PersonLevelAggregation; 