CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_DIABETES_DIAGNOSES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either DM_COD or DMRES_COD
    IS_DIABETES_DIAGNOSIS BOOLEAN, -- Flag indicating if this is a diabetes diagnosis code
    IS_DIABETES_RESOLVED BOOLEAN, -- Flag indicating if this is a diabetes resolved code
    EARLIEST_DIABETES_DATE DATE, -- Earliest diabetes diagnosis date
    LATEST_DIABETES_DATE DATE, -- Latest diabetes diagnosis date
    LATEST_RESOLUTION_DATE DATE, -- Latest diabetes resolution date
    IS_DIABETES_RESOLVED_FLAG BOOLEAN, -- Flag indicating if person's latest diabetes diagnosis is resolved
    ALL_DIABETES_CONCEPT_CODES ARRAY, -- All diabetes concept codes for this person
    ALL_DIABETES_CONCEPT_DISPLAYS ARRAY -- All diabetes concept display terms for this person
)
COMMENT = 'Intermediate table containing diabetes diagnoses and resolution status, using DM_COD and DMRES_COD clusters.'
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
        CASE WHEN MC.CLUSTER_ID = 'DM_COD' THEN O."clinical_effective_date"::DATE END AS DIABETES_DATE,
        CASE WHEN MC.CLUSTER_ID = 'DMRES_COD' THEN O."clinical_effective_date"::DATE END AS RESOLUTION_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('DM_COD', 'DMRES_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        MIN(DIABETES_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_DIABETES_DATE,
        MAX(DIABETES_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_DIABETES_DATE,
        MAX(RESOLUTION_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_RESOLUTION_DATE,
        -- Person's diabetes is resolved if their latest resolution date is after their latest diagnosis date
        CASE
            WHEN MAX(RESOLUTION_DATE) OVER (PARTITION BY PERSON_ID) >
                 MAX(DIABETES_DATE) OVER (PARTITION BY PERSON_ID) THEN TRUE
            ELSE FALSE
        END AS IS_DIABETES_RESOLVED_FLAG
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all diabetes concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_DIABETES_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_DIABETES_CONCEPT_DISPLAYS
    FROM BaseObservations
    WHERE SOURCE_CLUSTER_ID = 'DM_COD'
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
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'DM_COD' THEN TRUE ELSE FALSE END AS IS_DIABETES_DIAGNOSIS,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'DMRES_COD' THEN TRUE ELSE FALSE END AS IS_DIABETES_RESOLVED,
    pd.EARLIEST_DIABETES_DATE,
    pd.LATEST_DIABETES_DATE,
    pd.LATEST_RESOLUTION_DATE,
    pd.IS_DIABETES_RESOLVED_FLAG,
    c.ALL_DIABETES_CONCEPT_CODES,
    c.ALL_DIABETES_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
