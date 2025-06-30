CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_MH_DIAGNOSES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either MH_COD or MHREM_COD
    IS_DIAGNOSIS BOOLEAN, -- Flag indicating if this is a diagnosis code
    IS_REMISSION BOOLEAN, -- Flag indicating if this is a remission code
    EARLIEST_DIAGNOSIS_DATE DATE, -- Earliest mental health diagnosis date
    LATEST_DIAGNOSIS_DATE DATE, -- Latest mental health diagnosis date
    LATEST_REMISSION_DATE DATE, -- Latest remission date
    IS_IN_REMISSION BOOLEAN, -- Flag indicating if person is currently in remission
    ALL_MH_CONCEPT_CODES ARRAY, -- All mental health concept codes for this person
    ALL_MH_CONCEPT_DISPLAYS ARRAY -- All mental health concept display terms for this person
)
COMMENT = 'Intermediate table containing mental health diagnoses and remission status, using MH_COD and MHREM_COD clusters.'
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
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
        CASE WHEN MC.CLUSTER_ID = 'MH_COD' THEN O."clinical_effective_date"::DATE END AS DIAGNOSIS_DATE,
        CASE WHEN MC.CLUSTER_ID = 'MHREM_COD' THEN O."clinical_effective_date"::DATE END AS REMISSION_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('MH_COD', 'MHREM_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        MIN(DIAGNOSIS_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_DIAGNOSIS_DATE,
        MAX(DIAGNOSIS_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_DIAGNOSIS_DATE,
        MAX(REMISSION_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_REMISSION_DATE,
        -- Person is in remission if their latest remission date is after their latest diagnosis date
        CASE
            WHEN MAX(REMISSION_DATE) OVER (PARTITION BY PERSON_ID) >
                 MAX(DIAGNOSIS_DATE) OVER (PARTITION BY PERSON_ID) THEN TRUE
            ELSE FALSE
        END AS IS_IN_REMISSION
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all mental health concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_MH_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_MH_CONCEPT_DISPLAYS
    FROM BaseObservations
    WHERE SOURCE_CLUSTER_ID = 'MH_COD'
    GROUP BY PERSON_ID
)
-- Final selection with one row per person
SELECT
    pd.PERSON_ID,
    pd.SK_PATIENT_ID,
    pd.OBSERVATION_ID,
    pd.CLINICAL_EFFECTIVE_DATE,
    pd.CONCEPT_CODE,
    pd.CODE_DESCRIPTION,
    pd.SOURCE_CLUSTER_ID,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'MH_COD' THEN TRUE ELSE FALSE END AS IS_DIAGNOSIS,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'MHREM_COD' THEN TRUE ELSE FALSE END AS IS_REMISSION,
    pd.EARLIEST_DIAGNOSIS_DATE,
    pd.LATEST_DIAGNOSIS_DATE,
    pd.LATEST_REMISSION_DATE,
    pd.IS_IN_REMISSION,
    c.ALL_MH_CONCEPT_CODES,
    c.ALL_MH_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
