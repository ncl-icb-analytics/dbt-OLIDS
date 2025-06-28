CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_NDH_DIAGNOSES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either NDH_COD, IGT_COD, or PRD_COD
    IS_NDH_DIAGNOSIS BOOLEAN, -- Flag indicating if this is an NDH diagnosis code
    IS_IGT_DIAGNOSIS BOOLEAN, -- Flag indicating if this is an IGT diagnosis code
    IS_PRD_DIAGNOSIS BOOLEAN, -- Flag indicating if this is a PRD diagnosis code
    EARLIEST_NDH_DATE DATE, -- Earliest NDH diagnosis date
    EARLIEST_IGT_DATE DATE, -- Earliest IGT diagnosis date
    EARLIEST_PRD_DATE DATE, -- Earliest PRD diagnosis date
    LATEST_NDH_DATE DATE, -- Latest NDH diagnosis date
    LATEST_IGT_DATE DATE, -- Latest IGT diagnosis date
    LATEST_PRD_DATE DATE, -- Latest PRD diagnosis date
    EARLIEST_MULTNDH_DATE DATE, -- Earliest of NDH, IGT, or PRD diagnosis dates
    LATEST_MULTNDH_DATE DATE, -- Latest of NDH, IGT, or PRD diagnosis dates
    ALL_NDH_CONCEPT_CODES ARRAY, -- All NDH concept codes for this person
    ALL_NDH_CONCEPT_DISPLAYS ARRAY, -- All NDH concept display terms for this person
    ALL_IGT_CONCEPT_CODES ARRAY, -- All IGT concept codes for this person
    ALL_IGT_CONCEPT_DISPLAYS ARRAY, -- All IGT concept display terms for this person
    ALL_PRD_CONCEPT_CODES ARRAY, -- All PRD concept codes for this person
    ALL_PRD_CONCEPT_DISPLAYS ARRAY -- All PRD concept display terms for this person
)
COMMENT = 'Intermediate table containing NDH-related diagnoses (NDH, IGT, PRD codes) for the NDH register.'
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
        CASE WHEN MC.CLUSTER_ID = 'NDH_COD' THEN O."clinical_effective_date"::DATE END AS NDH_DATE,
        CASE WHEN MC.CLUSTER_ID = 'IGT_COD' THEN O."clinical_effective_date"::DATE END AS IGT_DATE,
        CASE WHEN MC.CLUSTER_ID = 'PRD_COD' THEN O."clinical_effective_date"::DATE END AS PRD_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('NDH_COD', 'IGT_COD', 'PRD_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        MIN(NDH_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_NDH_DATE,
        MIN(IGT_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_IGT_DATE,
        MIN(PRD_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_PRD_DATE,
        MAX(NDH_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_NDH_DATE,
        MAX(IGT_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_IGT_DATE,
        MAX(PRD_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_PRD_DATE,
        -- Calculate earliest and latest of all NDH-related diagnoses
        LEAST(
            MIN(NDH_DATE) OVER (PARTITION BY PERSON_ID),
            MIN(IGT_DATE) OVER (PARTITION BY PERSON_ID),
            MIN(PRD_DATE) OVER (PARTITION BY PERSON_ID)
        ) AS EARLIEST_MULTNDH_DATE,
        GREATEST(
            MAX(NDH_DATE) OVER (PARTITION BY PERSON_ID),
            MAX(IGT_DATE) OVER (PARTITION BY PERSON_ID),
            MAX(PRD_DATE) OVER (PARTITION BY PERSON_ID)
        ) AS LATEST_MULTNDH_DATE
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all concept codes and displays into arrays by type
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'NDH_COD' THEN CONCEPT_CODE END) AS ALL_NDH_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'NDH_COD' THEN CODE_DESCRIPTION END) AS ALL_NDH_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'IGT_COD' THEN CONCEPT_CODE END) AS ALL_IGT_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'IGT_COD' THEN CODE_DESCRIPTION END) AS ALL_IGT_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'PRD_COD' THEN CONCEPT_CODE END) AS ALL_PRD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'PRD_COD' THEN CODE_DESCRIPTION END) AS ALL_PRD_CONCEPT_DISPLAYS
    FROM BaseObservations
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
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'NDH_COD' THEN TRUE ELSE FALSE END AS IS_NDH_DIAGNOSIS,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'IGT_COD' THEN TRUE ELSE FALSE END AS IS_IGT_DIAGNOSIS,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'PRD_COD' THEN TRUE ELSE FALSE END AS IS_PRD_DIAGNOSIS,
    pd.EARLIEST_NDH_DATE,
    pd.EARLIEST_IGT_DATE,
    pd.EARLIEST_PRD_DATE,
    pd.LATEST_NDH_DATE,
    pd.LATEST_IGT_DATE,
    pd.LATEST_PRD_DATE,
    pd.EARLIEST_MULTNDH_DATE,
    pd.LATEST_MULTNDH_DATE,
    c.ALL_NDH_CONCEPT_CODES,
    c.ALL_NDH_CONCEPT_DISPLAYS,
    c.ALL_IGT_CONCEPT_CODES,
    c.ALL_IGT_CONCEPT_DISPLAYS,
    c.ALL_PRD_CONCEPT_CODES,
    c.ALL_PRD_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
