CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_OSTEOPOROSIS_DIAGNOSES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either OSTEO_COD, DXA_COD, or DXA2_COD
    IS_OSTEOPOROSIS_DIAGNOSIS BOOLEAN, -- Flag indicating if this is an osteoporosis diagnosis
    IS_DXA_SCAN BOOLEAN, -- Flag indicating if this is a DXA scan result
    IS_DXA_T_SCORE BOOLEAN, -- Flag indicating if this is a DXA T-score result
    DXA_T_SCORE NUMBER(10,1), -- The T-score value for DXA2_COD observations (1 decimal place)
    EARLIEST_OSTEOPOROSIS_DATE DATE, -- Earliest osteoporosis diagnosis date
    EARLIEST_DXA_DATE DATE, -- Earliest DXA scan date
    EARLIEST_DXA_T_SCORE_DATE DATE, -- Earliest DXA T-score date
    LATEST_OSTEOPOROSIS_DATE DATE, -- Latest osteoporosis diagnosis date
    LATEST_DXA_DATE DATE, -- Latest DXA scan date
    LATEST_DXA_T_SCORE_DATE DATE, -- Latest DXA T-score date
    ALL_OSTEOPOROSIS_CONCEPT_CODES ARRAY, -- All osteoporosis concept codes
    ALL_OSTEOPOROSIS_CONCEPT_DISPLAYS ARRAY, -- All osteoporosis concept display terms
    ALL_DXA_CONCEPT_CODES ARRAY, -- All DXA scan concept codes
    ALL_DXA_CONCEPT_DISPLAYS ARRAY -- All DXA scan concept display terms
)
COMMENT = 'Intermediate table containing osteoporosis diagnoses and DXA scans. Includes T-scores and handles osteoporosis diagnosis codes.'
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
        -- Extract T-score value for DXA2_COD observations
        CASE
            WHEN MC.CLUSTER_ID = 'DXA2_COD' THEN CAST(O."result_value"::FLOAT AS NUMBER(10,1))
            ELSE NULL
        END AS DXA_T_SCORE,
        -- Flag different types of observations
        CASE WHEN MC.CLUSTER_ID = 'OSTEO_COD' THEN O."clinical_effective_date"::DATE END AS OSTEO_DATE,
        CASE WHEN MC.CLUSTER_ID = 'DXA_COD' THEN O."clinical_effective_date"::DATE END AS DXA_DATE,
        CASE WHEN MC.CLUSTER_ID = 'DXA2_COD' THEN O."clinical_effective_date"::DATE END AS DXA_T_SCORE_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('OSTEO_COD', 'DXA_COD', 'DXA2_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        -- Flag different types of observations
        CASE WHEN SOURCE_CLUSTER_ID = 'OSTEO_COD' THEN TRUE ELSE FALSE END AS IS_OSTEOPOROSIS_DIAGNOSIS,
        CASE WHEN SOURCE_CLUSTER_ID = 'DXA_COD' THEN TRUE ELSE FALSE END AS IS_DXA_SCAN,
        CASE WHEN SOURCE_CLUSTER_ID = 'DXA2_COD' THEN TRUE ELSE FALSE END AS IS_DXA_T_SCORE,
        -- Get earliest dates
        MIN(OSTEO_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_OSTEOPOROSIS_DATE,
        MIN(DXA_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_DXA_DATE,
        MIN(DXA_T_SCORE_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_DXA_T_SCORE_DATE,
        -- Get latest dates
        MAX(OSTEO_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_OSTEOPOROSIS_DATE,
        MAX(DXA_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_DXA_DATE,
        MAX(DXA_T_SCORE_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_DXA_T_SCORE_DATE
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'OSTEO_COD' THEN CONCEPT_CODE END) AS ALL_OSTEOPOROSIS_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID = 'OSTEO_COD' THEN CODE_DESCRIPTION END) AS ALL_OSTEOPOROSIS_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID IN ('DXA_COD', 'DXA2_COD') THEN CONCEPT_CODE END) AS ALL_DXA_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN SOURCE_CLUSTER_ID IN ('DXA_COD', 'DXA2_COD') THEN CODE_DESCRIPTION END) AS ALL_DXA_CONCEPT_DISPLAYS
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
    pd.IS_OSTEOPOROSIS_DIAGNOSIS,
    pd.IS_DXA_SCAN,
    pd.IS_DXA_T_SCORE,
    pd.DXA_T_SCORE,
    pd.EARLIEST_OSTEOPOROSIS_DATE,
    pd.EARLIEST_DXA_DATE,
    pd.EARLIEST_DXA_T_SCORE_DATE,
    pd.LATEST_OSTEOPOROSIS_DATE,
    pd.LATEST_DXA_DATE,
    pd.LATEST_DXA_T_SCORE_DATE,
    c.ALL_OSTEOPOROSIS_CONCEPT_CODES,
    c.ALL_OSTEOPOROSIS_CONCEPT_DISPLAYS,
    c.ALL_DXA_CONCEPT_CODES,
    c.ALL_DXA_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
