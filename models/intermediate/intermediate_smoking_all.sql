CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either SMOK_COD, LSMOK_COD, EXSMOK_COD, or NSMOK_COD
    IS_CURRENT_SMOKER BOOLEAN, -- Flag indicating if this is a current smoker code
    IS_EX_SMOKER BOOLEAN, -- Flag indicating if this is an ex-smoker code
    IS_NEVER_SMOKED BOOLEAN, -- Flag indicating if this is a never smoked code
    EARLIEST_SMOKING_DATE DATE, -- Earliest smoking status date
    LATEST_SMOKING_DATE DATE, -- Latest smoking status date
    ALL_SMOKING_CONCEPT_CODES ARRAY, -- All smoking concept codes for this person
    ALL_SMOKING_CONCEPT_DISPLAYS ARRAY -- All smoking concept display terms for this person
)
COMMENT = 'Intermediate table containing all smoking status observations using QOF definitions. Includes current smokers, ex-smokers, and never smoked statuses. Uses cluster IDs: SMOK_COD (general smoking codes), LSMOK_COD (current smoker), EXSMOK_COD (ex-smoker), and NSMOK_COD (never smoked).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Selects all smoking-related observations
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
        -- Flag different types of smoking status
        CASE WHEN MC.CLUSTER_ID = 'LSMOK_COD' THEN TRUE ELSE FALSE END AS IS_CURRENT_SMOKER,
        CASE WHEN MC.CLUSTER_ID = 'EXSMOK_COD' THEN TRUE ELSE FALSE END AS IS_EX_SMOKER,
        CASE WHEN MC.CLUSTER_ID = 'NSMOK_COD' THEN TRUE ELSE FALSE END AS IS_NEVER_SMOKED
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('SMOK_COD', 'LSMOK_COD', 'EXSMOK_COD', 'NSMOK_COD')
),
PersonDates AS (
    -- Gets the earliest and latest smoking status dates for each person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        OBSERVATION_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CODE_DESCRIPTION,
        SOURCE_CLUSTER_ID,
        IS_CURRENT_SMOKER,
        IS_EX_SMOKER,
        IS_NEVER_SMOKED,
        MIN(CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_SMOKING_DATE,
        MAX(CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_SMOKING_DATE
    FROM BaseObservations
),
PersonLevelCodingAggregation AS (
    -- Aggregate all smoking concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_SMOKING_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_SMOKING_CONCEPT_DISPLAYS
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
    pd.IS_CURRENT_SMOKER,
    pd.IS_EX_SMOKER,
    pd.IS_NEVER_SMOKED,
    pd.EARLIEST_SMOKING_DATE,
    pd.LATEST_SMOKING_DATE,
    c.ALL_SMOKING_CONCEPT_CODES,
    c.ALL_SMOKING_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE DESC) = 1; 