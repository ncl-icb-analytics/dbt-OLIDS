CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_PREGNANT (
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    SEX,
    IS_CURRENTLY_PREGNANT,
    LATEST_PREG_COD_DATE,
    LATEST_PREGDEL_COD_DATE,
    ALL_PREG_OBSERVATION_IDS,
    ALL_PREG_CONCEPT_CODES,
    ALL_PREG_CONCEPT_DISPLAYS,
    ALL_PREG_SOURCE_CLUSTER_IDS,
    IS_CHILD_BEARING_AGE_12_55,
    IS_CHILD_BEARING_AGE_0_55
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseObservationsAndDemographics AS (
    -- Fetch relevant observations, joining directly to the pre-mapped concepts table
    -- Also join to person demographics to get AGE and SEX, and filter for non-males
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY, 
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID,
        age_dim.AGE,
        sex_dim.SEX
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_SEX sex_dim
        ON PP."person_id" = sex_dim.PERSON_ID
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age_dim
        ON PP."person_id" = age_dim.PERSON_ID
    WHERE 
        MC.CLUSTER_ID IN ('PREG_COD', 'PREGDEL_COD') AND MC.SOURCE = 'UKHSA_FLU'
        AND sex_dim.SEX != 'Male'
),
PersonLevelPregnancyAggregation AS (
    -- Aggregate pregnancy-related codes and dates at the person level
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID, 
        ANY_VALUE(AGE) as AGE, -- AGE from BaseObservationsAndDemographics
        ANY_VALUE(SEX) as SEX, -- SEX from BaseObservationsAndDemographics
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'PREG_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_PREG_COD_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'PREGDEL_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_PREGDEL_COD_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_PREG_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_PREG_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_PREG_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_PREG_SOURCE_CLUSTER_IDS
    FROM BaseObservationsAndDemographics
    GROUP BY PERSON_ID
)
-- Final assembly of pregnancy status
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    pla.AGE,
    pla.SEX,
    -- Determine if currently pregnant
    CASE
        WHEN pla.LATEST_PREG_COD_DATE IS NOT NULL AND
             pla.LATEST_PREG_COD_DATE >= DATEADD(month, -9, CURRENT_DATE()) AND
             (pla.LATEST_PREGDEL_COD_DATE IS NULL OR pla.LATEST_PREG_COD_DATE > pla.LATEST_PREGDEL_COD_DATE)
        THEN TRUE
        ELSE FALSE
    END AS IS_CURRENTLY_PREGNANT,
    pla.LATEST_PREG_COD_DATE,
    pla.LATEST_PREGDEL_COD_DATE,
    pla.ALL_PREG_OBSERVATION_IDS,
    pla.ALL_PREG_CONCEPT_CODES,
    pla.ALL_PREG_CONCEPT_DISPLAYS,
    pla.ALL_PREG_SOURCE_CLUSTER_IDS,
    -- Recalculate child-bearing age flags based on pla.AGE (SEX is already != 'Male' due to filter in BaseObservationsAndDemographics)
    (pla.AGE >= 12 AND pla.AGE <= 55) AS IS_CHILD_BEARING_AGE_12_55,
    (pla.AGE <= 55) AS IS_CHILD_BEARING_AGE_0_55 -- Age is non-negative, so pla.AGE >= 0 is implied
FROM PersonLevelPregnancyAggregation pla
WHERE IS_CURRENTLY_PREGNANT = TRUE; -- Filter to only include those currently deemed pregnant 