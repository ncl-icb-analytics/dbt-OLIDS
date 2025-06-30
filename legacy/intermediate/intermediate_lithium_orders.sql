CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_LITHIUM_ORDERS (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either LIT_COD or LITSP_COD
    IS_LITHIUM_ORDER BOOLEAN, -- Flag indicating if this is a lithium order
    IS_LITHIUM_STOPPED BOOLEAN, -- Flag indicating if this is a lithium stopped code
    LATEST_LITHIUM_ORDER_DATE DATE, -- Latest lithium order date
    LATEST_LITHIUM_STOPPED_DATE DATE, -- Latest lithium stopped date
    LITHIUM_ISSUED_LAST_6M BOOLEAN, -- TRUE if person has a lithium order in the last 6 months and not stopped (see logic)
    ALL_LITHIUM_CONCEPT_CODES ARRAY, -- All lithium concept codes for this person
    ALL_LITHIUM_CONCEPT_DISPLAYS ARRAY -- All lithium concept display terms for this person
)
COMMENT = 'Intermediate table containing all lithium orders and stopped status, using LIT_COD and LITSP_COD clusters. Includes a flag (LITHIUM_ISSUED_LAST_6M) for whether a person has a lithium order in the last 6 months and not stopped, but does not filter on this.'
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
        CASE WHEN MC.CLUSTER_ID = 'LIT_COD' THEN O."clinical_effective_date"::DATE END AS ORDER_DATE,
        CASE WHEN MC.CLUSTER_ID = 'LITSP_COD' THEN O."clinical_effective_date"::DATE END AS STOPPED_DATE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('LIT_COD', 'LITSP_COD')
),
PersonDates AS (
    SELECT
        bo.*,
        MAX(ORDER_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_LITHIUM_ORDER_DATE,
        MAX(STOPPED_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_LITHIUM_STOPPED_DATE,
        -- Person has lithium issued in last 6 months if:
        -- 1. They have a lithium order in the last 6 months
        -- 2. AND either no stopped date OR stopped date is before the latest order
        CASE
            WHEN MAX(ORDER_DATE) OVER (PARTITION BY PERSON_ID) >= DATEADD(month, -6, CURRENT_DATE())
                AND (
                    MAX(STOPPED_DATE) OVER (PARTITION BY PERSON_ID) IS NULL
                    OR MAX(STOPPED_DATE) OVER (PARTITION BY PERSON_ID) < MAX(ORDER_DATE) OVER (PARTITION BY PERSON_ID)
                ) THEN TRUE
            ELSE FALSE
        END AS LITHIUM_ISSUED_LAST_6M
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all lithium concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_LITHIUM_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_LITHIUM_CONCEPT_DISPLAYS
    FROM BaseObservations
    WHERE SOURCE_CLUSTER_ID = 'LIT_COD'
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
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'LIT_COD' THEN TRUE ELSE FALSE END AS IS_LITHIUM_ORDER,
    CASE WHEN pd.SOURCE_CLUSTER_ID = 'LITSP_COD' THEN TRUE ELSE FALSE END AS IS_LITHIUM_STOPPED,
    pd.LATEST_LITHIUM_ORDER_DATE,
    pd.LATEST_LITHIUM_STOPPED_DATE,
    pd.LITHIUM_ISSUED_LAST_6M,
    c.ALL_LITHIUM_CONCEPT_CODES,
    c.ALL_LITHIUM_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
