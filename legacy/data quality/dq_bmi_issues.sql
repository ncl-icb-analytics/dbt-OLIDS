CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DQ_BMI_ISSUES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either BMI30_COD (implies BMI >= 30) or BMIVAL_COD (any numeric BMI value)
    BMI_VALUE NUMBER, -- The numeric BMI value (30 for BMI30_COD, actual value for BMIVAL_COD)
    IS_TOO_LOW BOOLEAN, -- Flag indicating if BMI is below 5
    IS_TOO_HIGH BOOLEAN, -- Flag indicating if BMI is above 400
    ISSUE_DESCRIPTION VARCHAR -- Description of the data quality issue
)
COMMENT = 'Data quality table tracking BMI values outside the valid range (5-400). Includes all BMI values from both BMI30_COD (>=30) and BMIVAL_COD (any numeric value) sources. Used to monitor and investigate potential data quality issues.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all BMI observations, including both:
    -- 1. BMI30_COD: Codes indicating BMI >= 30 (assigned value of 30)
    -- 2. BMIVAL_COD: Actual numeric BMI values of any value
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
        -- Extract BMI value from result_value, handling both types:
        -- - BMI30_COD: Always assigned value of 30
        -- - BMIVAL_COD: Actual numeric value from result_value
        CASE
            WHEN MC.CLUSTER_ID = 'BMIVAL_COD' THEN CAST(O."result_value"::FLOAT AS NUMBER(10,2))
            WHEN MC.CLUSTER_ID = 'BMI30_COD' THEN 30 -- BMI30_COD implies BMI >= 30
            ELSE NULL
        END AS BMI_VALUE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('BMI30_COD', 'BMIVAL_COD') -- Include all BMI values from both sources
)
-- Final selection identifying BMI values outside valid range
SELECT
    bo.PERSON_ID,
    bo.SK_PATIENT_ID,
    bo.OBSERVATION_ID,
    bo.CLINICAL_EFFECTIVE_DATE,
    bo.CONCEPT_CODE,
    bo.CODE_DESCRIPTION,
    bo.SOURCE_CLUSTER_ID,
    bo.BMI_VALUE,
    CASE WHEN bo.BMI_VALUE < 5 THEN TRUE ELSE FALSE END AS IS_TOO_LOW,
    CASE WHEN bo.BMI_VALUE > 400 THEN TRUE ELSE FALSE END AS IS_TOO_HIGH,
    CASE
        WHEN bo.BMI_VALUE < 5 THEN 'BMI value below valid range (<5)'
        WHEN bo.BMI_VALUE > 400 THEN 'BMI value above valid range (>400)'
        ELSE NULL
    END AS ISSUE_DESCRIPTION
FROM BaseObservations bo
WHERE bo.BMI_VALUE < 5 OR bo.BMI_VALUE > 400; -- Only include values outside valid range
