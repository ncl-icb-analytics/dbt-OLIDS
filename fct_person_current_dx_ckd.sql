-- Define or replace the dynamic table in the HEI_MIGRATION schema to store ONLY patients currently on the CKD register.
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_CKD (
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    -- IS_ON_CKD_REGISTER, -- Implicitly TRUE due to final filter
    -- Latest Lab Info from Inference Table
    LATEST_EGFR_VALUE,
    LATEST_EGFR_DATE,
    LATEST_ACR_VALUE,
    LATEST_ACR_DATE,
    LATEST_EGFR_STAGE,
    LATEST_ACR_STAGE,
    LATEST_CKD_STAGE_INFERRED,
    LATEST_LABS_MEET_CKD_CRITERIA,
    -- Confirmation Flags from Inference Table
    HAS_CONFIRMED_LOW_EGFR,
    HAS_CONFIRMED_HIGH_ACR,
    HAS_CONFIRMED_CKD_BY_LABS,
    -- Dates from Coding
    EARLIEST_CKD_DIAGNOSIS_DATE,
    LATEST_CKD_DIAGNOSIS_DATE,
    LATEST_CKD_RESOLVED_DATE,
    -- Aggregated details for traceability of codes
    ALL_CKD_OBSERVATION_IDS,
    ALL_CKD_CONCEPT_CODES,
    ALL_CKD_CONCEPT_DISPLAYS,
    ALL_CKD_SOURCE_CLUSTER_IDS
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS

AS

WITH BaseObservationsAndClusters AS (
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('CKD_COD', 'CKDRES_COD')
),
FilteredByAge AS (
    -- Join with age dimension and filter for age >= 18
    SELECT
        boc.*,
        age.AGE
    FROM BaseObservationsAndClusters boc
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        ON boc.PERSON_ID = age.PERSON_ID
    WHERE age.AGE >= 18
),
PersonLevelCKDCodingAggregation AS (
    -- Aggregate CKD code information per person
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'CKD_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_CKD_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'CKD_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_CKD_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'CKDRES_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_CKD_RESOLVED_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_CKD_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_CKD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_CKD_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_CKD_SOURCE_CLUSTER_IDS,
        -- Calculate register status based on codes
        CASE
            WHEN LATEST_CKD_DIAGNOSIS_DATE IS NOT NULL AND
                 (LATEST_CKD_RESOLVED_DATE IS NULL OR LATEST_CKD_DIAGNOSIS_DATE > LATEST_CKD_RESOLVED_DATE)
            THEN TRUE
            ELSE FALSE
        END AS IS_ON_CKD_REGISTER_CALC
    FROM FilteredByAge
    GROUP BY PERSON_ID
)
-- Final selection, joining coding status with lab inference and filtering
SELECT
    cod_agg.PERSON_ID,
    cod_agg.SK_PATIENT_ID,
    cod_agg.AGE,
    -- Lab Inference Details
    lab_inf.LATEST_EGFR_VALUE,
    lab_inf.LATEST_EGFR_DATE,
    lab_inf.LATEST_ACR_VALUE,
    lab_inf.LATEST_ACR_DATE,
    lab_inf.LATEST_EGFR_STAGE,
    lab_inf.LATEST_ACR_STAGE,
    lab_inf.LATEST_CKD_STAGE_INFERRED,
    lab_inf.LATEST_LABS_MEET_CKD_CRITERIA,
    lab_inf.HAS_CONFIRMED_LOW_EGFR,
    lab_inf.HAS_CONFIRMED_HIGH_ACR,
    lab_inf.HAS_CONFIRMED_CKD_BY_LABS,
    -- Coding Dates
    cod_agg.EARLIEST_CKD_DIAGNOSIS_DATE,
    cod_agg.LATEST_CKD_DIAGNOSIS_DATE,
    cod_agg.LATEST_CKD_RESOLVED_DATE,
    -- Coding Traceability
    cod_agg.ALL_CKD_OBSERVATION_IDS,
    cod_agg.ALL_CKD_CONCEPT_CODES,
    cod_agg.ALL_CKD_CONCEPT_DISPLAYS,
    cod_agg.ALL_CKD_SOURCE_CLUSTER_IDS
FROM PersonLevelCKDCodingAggregation cod_agg
-- Left join to lab inference table to get lab status for those on register
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_CKD_LAB_INFERENCE lab_inf
    ON cod_agg.PERSON_ID = lab_inf.PERSON_ID
-- Filter to include only those currently on the CKD register based on coding
WHERE cod_agg.IS_ON_CKD_REGISTER_CALC = TRUE;
