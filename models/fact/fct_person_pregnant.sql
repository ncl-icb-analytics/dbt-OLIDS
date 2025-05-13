CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_PREGNANT (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    SEX VARCHAR, -- Sex of the person (filtered to be non-'Male')
    IS_CURRENTLY_PREGNANT BOOLEAN, -- Flag indicating if the person is currently deemed pregnant
    LATEST_PREG_COD_DATE DATE, -- Latest date of a pregnancy code (PREG_COD) within the last 9 months
    LATEST_PREGDEL_COD_DATE DATE, -- Latest date of a pregnancy ended/delivery code (PREGDEL_COD)
    ALL_PREG_OBSERVATION_IDS ARRAY, -- Array of all observation IDs related to pregnancy for the person
    ALL_PREG_CONCEPT_CODES ARRAY, -- Array of all pregnancy-related concept codes recorded
    ALL_PREG_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for pregnancy-related codes
    ALL_PREG_SOURCE_CLUSTER_IDS ARRAY, -- Array of source cluster IDs (PREG_COD, PREGDEL_COD)
    IS_CHILD_BEARING_AGE_12_55 BOOLEAN, -- Flag: TRUE if age is 12-55 inclusive
    IS_CHILD_BEARING_AGE_0_55 BOOLEAN,  -- Flag: TRUE if age is 0-55 inclusive
    HAS_PERMANENT_ABSENCE_PREG_RISK_FLAG BOOLEAN -- Flag: TRUE if the person has a record in INTERMEDIATE_PERM_ABSENCE_PREG_RISK
)
COMMENT = 'Fact table identifying non-male individuals currently deemed pregnant based on PREG_COD and PREGDEL_COD codes from UKHSA_FLU source within the last 9 months. Includes a flag for permanent absence of pregnancy risk.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseObservationsAndDemographics AS (
    -- Fetches observation records for pregnancy (PREG_COD) or pregnancy ended/delivery (PREGDEL_COD) from the UKHSA_FLU source.
    -- Joins with patient demographics (age and sex) and filters for individuals not recorded as 'Male'.
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
    -- Aggregates pregnancy-related information for each non-male individual.
    -- Determines the latest dates for PREG_COD and PREGDEL_COD.
    -- Collects all associated observation details into arrays.
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
-- Final assembly of pregnancy status.
-- Determines IS_CURRENTLY_PREGNANT based on the logic: a recent PREG_COD (last 9 months) that is later than any PREGDEL_COD.
-- Includes age-based child-bearing flags and a flag for permanent absence of pregnancy risk.
-- Filters to only include those currently deemed pregnant.
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    pla.AGE,
    pla.SEX,
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
    (pla.AGE >= 12 AND pla.AGE <= 55) AS IS_CHILD_BEARING_AGE_12_55,
    (pla.AGE <= 55) AS IS_CHILD_BEARING_AGE_0_55,
    (perm_abs.PERSON_ID IS NOT NULL) AS HAS_PERMANENT_ABSENCE_PREG_RISK_FLAG
FROM PersonLevelPregnancyAggregation pla
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PERM_ABSENCE_PREG_RISK perm_abs
    ON pla.PERSON_ID = perm_abs.PERSON_ID
WHERE IS_CURRENTLY_PREGNANT = TRUE; -- Filter to only include those currently deemed pregnant 