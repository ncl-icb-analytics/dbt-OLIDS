-- Define or replace the dynamic table in the HEI_MIGRATION schema to store ONLY patients currently on the CKD register.
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_CKD (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (>= 18 for this table)
    IS_ON_CKD_REGISTER BOOLEAN, -- Flag indicating if the person is currently on the CKD register
    -- Latest Lab Info from INTERMEDIATE_CKD_LAB_INFERENCE
    LATEST_EGFR_VALUE NUMBER, -- Latest eGFR value recorded
    LATEST_EGFR_DATE DATE, -- Date of the latest eGFR value
    LATEST_ACR_VALUE NUMBER, -- Latest ACR (Albumin-to-Creatinine Ratio) value recorded
    LATEST_ACR_DATE DATE, -- Date of the latest ACR value
    LATEST_EGFR_STAGE VARCHAR, -- CKD stage based on the latest eGFR value
    LATEST_ACR_STAGE VARCHAR, -- CKD albuminuria stage based on the latest ACR value
    LATEST_CKD_STAGE_INFERRED VARCHAR, -- Overall CKD stage inferred from latest eGFR and ACR (e.g., G3aA1)
    LATEST_LABS_MEET_CKD_CRITERIA BOOLEAN, -- Flag: TRUE if latest labs (eGFR/ACR) meet criteria for CKD diagnosis without confirmation over time
    -- Confirmation Flags from INTERMEDIATE_CKD_LAB_INFERENCE (indicating persistence over >90 days)
    HAS_CONFIRMED_LOW_EGFR BOOLEAN, -- Flag: TRUE if persistently low eGFR (meeting CKD criteria) is confirmed over >90 days
    HAS_CONFIRMED_HIGH_ACR BOOLEAN, -- Flag: TRUE if persistently high ACR (meeting CKD criteria) is confirmed over >90 days
    HAS_CONFIRMED_CKD_BY_LABS BOOLEAN, -- Flag: TRUE if CKD is confirmed by persistent lab results (either low eGFR or high ACR)
    -- Dates from CKD Coding (CKD_COD, CKDRES_COD)
    EARLIEST_CKD_DIAGNOSIS_DATE DATE, -- Earliest recorded date of a CKD diagnosis code (CKD_COD)
    LATEST_CKD_DIAGNOSIS_DATE DATE, -- Latest recorded date of a CKD diagnosis code (CKD_COD)
    LATEST_CKD_RESOLVED_DATE DATE, -- Latest recorded date of a CKD resolved code (CKDRES_COD)
    -- Aggregated details for traceability of CKD codes
    ALL_CKD_CONCEPT_CODES ARRAY, -- Array of all CKD_COD/CKDRES_COD concept codes recorded for the person
    ALL_CKD_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the CKD_COD/CKDRES_COD concept codes
    ALL_CKD_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (CKD_COD, CKDRES_COD)
)
COMMENT = 'Fact table identifying individuals aged 18 and over currently on the Chronic Kidney Disease (CKD) register based on coding (latest CKD_COD > latest CKDRES_COD). It enriches these records with the latest CKD-related lab results and inferred CKD stage/confirmation status from INTERMEDIATE_CKD_LAB_INFERENCE.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS

AS

WITH BaseObservationsAndClusters AS (
    -- Fetches observation records related to CKD diagnosis (CKD_COD) or resolution (CKDRES_COD).
    -- Includes basic person identifiers and clinical effective dates.
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY,
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('CKD_COD', 'CKDRES_COD')
),
FilteredByAge AS (
    -- Filters the base CKD-related observations to include only individuals aged 18 or older.
    SELECT
        boc.*,
        age.AGE
    FROM BaseObservationsAndClusters boc
    JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE age
        ON boc.PERSON_ID = age.PERSON_ID
    WHERE age.AGE >= 18
),
PersonLevelCKDCodingAggregation AS (
    -- Aggregates CKD diagnosis and resolution code information for each person aged 18+.
    -- Calculates earliest/latest CKD_COD dates and latest CKDRES_COD date.
    -- Determines IS_ON_CKD_REGISTER: TRUE if there's an active CKD_COD (latest CKD_COD > latest CKDRES_COD, or no CKDRES_COD).
    -- Collects all associated concept details (codes, displays, cluster IDs) into arrays.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'CKD_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_CKD_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'CKD_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_CKD_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'CKDRES_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_CKD_RESOLVED_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_CKD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_CKD_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_CKD_SOURCE_CLUSTER_IDS,
        CASE
            WHEN LATEST_CKD_DIAGNOSIS_DATE IS NOT NULL AND
                 (LATEST_CKD_RESOLVED_DATE IS NULL OR LATEST_CKD_DIAGNOSIS_DATE > LATEST_CKD_RESOLVED_DATE)
            THEN TRUE
            ELSE FALSE
        END AS IS_ON_CKD_REGISTER
    FROM FilteredByAge
    GROUP BY PERSON_ID
)
-- Final selection: Combines CKD coding status with CKD lab inference data.
-- Filters to include only individuals currently on the CKD register based on their coding status.
SELECT
    cod_agg.PERSON_ID,
    cod_agg.SK_PATIENT_ID,
    cod_agg.AGE,
    cod_agg.IS_ON_CKD_REGISTER,
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
    cod_agg.ALL_CKD_CONCEPT_CODES,
    cod_agg.ALL_CKD_CONCEPT_DISPLAYS,
    cod_agg.ALL_CKD_SOURCE_CLUSTER_IDS
FROM PersonLevelCKDCodingAggregation cod_agg
-- Left join to the intermediate lab inference table to bring in lab results and CKD stage/confirmation details.
-- This join enriches coded CKD patients with their latest lab-based CKD status.
LEFT JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_CKD_LAB_INFERENCE lab_inf
    ON cod_agg.PERSON_ID = lab_inf.PERSON_ID
-- Final filter to ensure only individuals currently on the CKD register (based on coding logic) are included in this fact table.
WHERE cod_agg.IS_ON_CKD_REGISTER = TRUE;
