CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_RA (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_RA_REGISTER BOOLEAN, -- Flag indicating if person is on the RA register (always TRUE for rows in this table)
    EARLIEST_RA_DIAGNOSIS_DATE DATE, -- Earliest RA diagnosis date
    LATEST_RA_DIAGNOSIS_DATE DATE, -- Latest RA diagnosis date
    ALL_RA_CONCEPT_CODES ARRAY, -- All RA concept codes
    ALL_RA_CONCEPT_DISPLAYS ARRAY -- All RA concept display terms
)
COMMENT = 'Fact table identifying individuals aged 16 or over with a diagnosis of Rheumatoid Arthritis (RARTH_COD).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all RA diagnoses for patients aged 16 or over
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
    WHERE MC.CLUSTER_ID = 'RARTH_COD'
      AND AGE.AGE >= 16
),
PersonLevelAggregation AS (
    -- Aggregate to one row per person with earliest diagnosis date and concept arrays
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        AGE,
        MIN(CLINICAL_EFFECTIVE_DATE) AS EARLIEST_RA_DIAGNOSIS_DATE,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_RA_DIAGNOSIS_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_RA_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_RA_CONCEPT_DISPLAYS
    FROM BaseObservations
    GROUP BY PERSON_ID, SK_PATIENT_ID, AGE
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    TRUE AS IS_ON_RA_REGISTER, -- All patients in this table are on the register
    EARLIEST_RA_DIAGNOSIS_DATE,
    LATEST_RA_DIAGNOSIS_DATE,
    ALL_RA_CONCEPT_CODES,
    ALL_RA_CONCEPT_DISPLAYS
FROM PersonLevelAggregation;
