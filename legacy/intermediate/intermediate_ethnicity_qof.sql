CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_ETHNICITY_QOF (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the observation
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- The ethnicity cluster ID
    IS_BAME BOOLEAN, -- Flag indicating if ethnicity is BAME
    LATEST_ETHNICITY_DATE DATE, -- Latest ethnicity recording date
    LATEST_BAME_DATE DATE, -- Latest BAME ethnicity recording date
    ALL_ETHNICITY_CONCEPT_CODES ARRAY, -- All ethnicity concept codes for this person
    ALL_ETHNICITY_CONCEPT_DISPLAYS ARRAY -- All ethnicity concept display terms for this person
)
COMMENT = 'Intermediate table containing ethnicity information from observations only, with BAME status flags for obesity register.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH JournalEthnicity AS (
    -- Get ethnicity codes from journal entries
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
        -- Flag for BAME ethnicity based on cluster IDs
        CASE
            WHEN MC.CLUSTER_ID IN (
                'ETH2016MWBC_COD', -- White and Black Caribbean
                'ETH2016MWBA_COD', -- White and Black African
                'ETH2016MWA_COD',  -- White and Asian
                'ETH2016AI_COD',   -- Indian
                'ETH2016AP_COD',   -- Pakistani
                'ETH2016AB_COD',   -- Bangladeshi
                'ETH2016AC_COD',   -- Chinese
                'ETH2016AO_COD',   -- Any other Asian background
                'ETH2016BA_COD',   -- African
                'ETH2016BC_COD',   -- Caribbean
                'ETH2016BO_COD',   -- Any other Black or African or Caribbean background
                'ETH2016OA_COD'    -- Arab
            ) THEN TRUE
            ELSE FALSE
        END AS IS_BAME
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID LIKE 'ETH2016%_COD'
),
PersonDates AS (
    SELECT
        je.*,
        -- Get latest ethnicity dates
        MAX(CLINICAL_EFFECTIVE_DATE) OVER (PARTITION BY PERSON_ID) AS LATEST_ETHNICITY_DATE,
        MAX(CASE WHEN IS_BAME THEN CLINICAL_EFFECTIVE_DATE END)
            OVER (PARTITION BY PERSON_ID) AS LATEST_BAME_DATE
    FROM JournalEthnicity je
),
PersonLevelCodingAggregation AS (
    -- Aggregate all ethnicity concept codes and displays into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_ETHNICITY_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_ETHNICITY_CONCEPT_DISPLAYS
    FROM JournalEthnicity
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
    pd.IS_BAME,
    pd.LATEST_ETHNICITY_DATE,
    pd.LATEST_BAME_DATE,
    c.ALL_ETHNICITY_CONCEPT_CODES,
    c.ALL_ETHNICITY_CONCEPT_DISPLAYS
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
