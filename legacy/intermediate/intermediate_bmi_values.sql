CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_BMI_VALUES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either BMI30_COD or BMIVAL_COD
    BMI_VALUE NUMBER, -- The numeric BMI value
    IS_BMI_30_PLUS BOOLEAN, -- Flag indicating if this is a BMI30_COD code or value >= 30
    IS_BMI_27_5_PLUS BOOLEAN, -- Flag indicating if value >= 27.5
    IS_VALID_BMI BOOLEAN, -- Flag indicating if BMI is within valid range (5-400)
    LATEST_BMI_DATE DATE, -- Latest BMI recording date
    LATEST_VALID_BMI_DATE DATE, -- Latest valid BMI recording date
    LATEST_VALID_BMI_VALUE NUMBER, -- Latest valid BMI value
    ALL_BMI_CONCEPT_CODES ARRAY, -- All BMI concept codes for this person
    ALL_BMI_CONCEPT_DISPLAYS ARRAY -- All BMI concept display terms for this person
)
COMMENT = 'Intermediate table containing BMI values with data quality checks. Includes latest BMI values and handles both numeric values and BMI30_COD codes. Filters out extreme values (<5 or >400) and tracks both latest and latest valid BMI values.'
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
        -- Extract BMI value from result_value, handling both numeric and coded values
        CASE
            WHEN MC.CLUSTER_ID = 'BMIVAL_COD' THEN CAST(O."result_value"::FLOAT AS NUMBER(10,2))
            WHEN MC.CLUSTER_ID = 'BMI30_COD' THEN 30 -- BMI30_COD implies BMI >= 30
            ELSE NULL
        END AS BMI_VALUE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('BMI30_COD', 'BMIVAL_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        -- Flag for valid BMI range (5-400)
        CASE
            WHEN BMI_VALUE BETWEEN 5 AND 400 THEN TRUE
            ELSE FALSE
        END AS IS_VALID_BMI,
        -- Flag for BMI >= 30
        CASE
            WHEN SOURCE_CLUSTER_ID = 'BMI30_COD' OR BMI_VALUE >= 30 THEN TRUE
            ELSE FALSE
        END AS IS_BMI_30_PLUS,
        -- Flag for BMI >= 27.5
        CASE
            WHEN BMI_VALUE >= 27.5 THEN TRUE
            ELSE FALSE
        END AS IS_BMI_27_5_PLUS,
        -- Get latest BMI dates and values
        MAX(CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_BMI_DATE,
        MAX(CASE WHEN BMI_VALUE BETWEEN 5 AND 400 THEN CLINICAL_EFFECTIVE_DATE END)
            OVER (PARTITION BY PERSON_ID) AS LATEST_VALID_BMI_DATE,
        MAX(CASE WHEN BMI_VALUE BETWEEN 5 AND 400 THEN BMI_VALUE END)
            OVER (PARTITION BY PERSON_ID) AS LATEST_VALID_BMI_VALUE
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all BMI concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_BMI_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_BMI_CONCEPT_DISPLAYS
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
    pd.BMI_VALUE,
    pd.IS_BMI_30_PLUS,
    pd.IS_BMI_27_5_PLUS,
    pd.IS_VALID_BMI,
    pd.LATEST_BMI_DATE,
    pd.LATEST_VALID_BMI_DATE,
    pd.LATEST_VALID_BMI_VALUE,
    c.ALL_BMI_CONCEPT_CODES,
    c.ALL_BMI_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
