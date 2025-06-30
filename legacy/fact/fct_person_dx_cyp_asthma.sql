CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_CYP_ASTHMA (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (0 to under 18 years)
    IS_ON_ASTHMA_REGISTER BOOLEAN, -- Flag indicating if the person is currently on the asthma register
    -- Dates from Asthma Coding (AST_COD, ASTRES_COD)
    EARLIEST_ASTHMA_DIAGNOSIS_DATE DATE, -- Earliest recorded date of an asthma diagnosis code (AST_COD)
    LATEST_ASTHMA_DIAGNOSIS_DATE DATE, -- Latest recorded date of an asthma diagnosis code (AST_COD)
    LATEST_ASTHMA_RESOLVED_DATE DATE, -- Latest recorded date of an asthma resolved code (ASTRES_COD)
    -- Latest Medication Details
    LATEST_ASTHMA_MED_ORDER_DATE DATE, -- Date of the most recent asthma medication order
    LATEST_ASTHMA_MED_NAME VARCHAR, -- Name of the most recent asthma medication
    LATEST_ASTHMA_MED_DOSE VARCHAR, -- Dose of the most recent asthma medication
    LATEST_ASTHMA_MED_CONCEPT_CODE VARCHAR, -- Concept code of the most recent asthma medication
    LATEST_ASTHMA_MED_CONCEPT_DISPLAY VARCHAR, -- Display term for the most recent asthma medication
    RECENT_ASTHMA_MED_ORDER_COUNT NUMBER, -- Count of asthma medication orders in the last 12 months
    -- Aggregated details for traceability
    ALL_ASTHMA_CONCEPT_CODES ARRAY, -- Array of all AST_COD/ASTRES_COD concept codes recorded for the person
    ALL_ASTHMA_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the AST_COD/ASTRES_COD concept codes
    ALL_ASTHMA_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (AST_COD, ASTRES_COD)
)
COMMENT = 'Fact table identifying individuals aged 0 to under 18 years (not including 18) currently on the Asthma register. Requires both an active asthma diagnosis (latest AST_COD > latest ASTRES_COD) and a recent asthma medication order (within last 12 months). This table is designed for general analysis of asthma patterns in children and young people, while the LTC LCS indicator (CYP_AST_61) applies additional age restrictions (18 months to under 18 years) for case-finding purposes.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservationsAndClusters AS (
    -- Fetches observation records related to asthma diagnosis (AST_COD) or resolution (ASTRES_COD).
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
    WHERE MC.CLUSTER_ID IN ('AST_COD', 'ASTRES_COD')
),
FilteredByAge AS (
    -- Filters the base asthma-related observations to include only individuals aged 0 to under 18 years
    -- Note: This is more inclusive than the LTC LCS indicator to allow for broader analysis
    SELECT
        boc.*,
        age.AGE
    FROM BaseObservationsAndClusters boc
    JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE age
        ON boc.PERSON_ID = age.PERSON_ID
    WHERE age.AGE < 18  -- Include all ages 0 to under 18 for general analysis
),
PersonLevelAsthmaCodingAggregation AS (
    -- Aggregates asthma diagnosis and resolution code information for each person aged 0 to under 18 years
    -- Calculates earliest/latest AST_COD dates and latest ASTRES_COD date
    -- Determines IS_ON_ASTHMA_REGISTER: TRUE if there's an active AST_COD (latest AST_COD > latest ASTRES_COD, or no ASTRES_COD)
    -- Collects all associated concept details (codes, displays, cluster IDs) into arrays
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'AST_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_ASTHMA_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'AST_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_ASTHMA_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'ASTRES_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_ASTHMA_RESOLVED_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_ASTHMA_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_ASTHMA_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_ASTHMA_SOURCE_CLUSTER_IDS,
        CASE
            WHEN LATEST_ASTHMA_DIAGNOSIS_DATE IS NOT NULL AND
                 (LATEST_ASTHMA_RESOLVED_DATE IS NULL OR LATEST_ASTHMA_DIAGNOSIS_DATE > LATEST_ASTHMA_RESOLVED_DATE)
            THEN TRUE
            ELSE FALSE
        END AS HAS_ACTIVE_ASTHMA_DIAGNOSIS
    FROM FilteredByAge
    GROUP BY PERSON_ID
),
LatestMedicationDetails AS (
    -- Gets the most recent asthma medication order for each person
    SELECT
        PERSON_ID,
        MAX(ORDER_DATE) as LATEST_ASTHMA_MED_ORDER_DATE,
        ANY_VALUE(ORDER_MEDICATION_NAME) as LATEST_ASTHMA_MED_NAME,
        ANY_VALUE(ORDER_DOSE) as LATEST_ASTHMA_MED_DOSE,
        ANY_VALUE(MAPPED_CONCEPT_CODE) as LATEST_ASTHMA_MED_CONCEPT_CODE,
        ANY_VALUE(MAPPED_CONCEPT_DISPLAY) as LATEST_ASTHMA_MED_CONCEPT_DISPLAY,
        ANY_VALUE(RECENT_ORDER_COUNT) as RECENT_ASTHMA_MED_ORDER_COUNT
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_ASTHMA_ORDERS_12M
    GROUP BY PERSON_ID
)
-- Final selection: Combines asthma coding status with medication details
-- Filters to include only individuals who meet both criteria:
-- 1. Have an active asthma diagnosis (latest AST_COD > latest ASTRES_COD)
-- 2. Have a recent asthma medication order (within last 12 months)
SELECT
    cod_agg.PERSON_ID,
    cod_agg.SK_PATIENT_ID,
    cod_agg.AGE,
    CASE
        WHEN cod_agg.HAS_ACTIVE_ASTHMA_DIAGNOSIS = TRUE
        AND med.LATEST_ASTHMA_MED_ORDER_DATE IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS IS_ON_ASTHMA_REGISTER,
    -- Coding Dates
    cod_agg.EARLIEST_ASTHMA_DIAGNOSIS_DATE,
    cod_agg.LATEST_ASTHMA_DIAGNOSIS_DATE,
    cod_agg.LATEST_ASTHMA_RESOLVED_DATE,
    -- Latest Medication Details
    med.LATEST_ASTHMA_MED_ORDER_DATE,
    med.LATEST_ASTHMA_MED_NAME,
    med.LATEST_ASTHMA_MED_DOSE,
    med.LATEST_ASTHMA_MED_CONCEPT_CODE,
    med.LATEST_ASTHMA_MED_CONCEPT_DISPLAY,
    med.RECENT_ASTHMA_MED_ORDER_COUNT,
    -- Coding Traceability
    cod_agg.ALL_ASTHMA_CONCEPT_CODES,
    cod_agg.ALL_ASTHMA_CONCEPT_DISPLAYS,
    cod_agg.ALL_ASTHMA_SOURCE_CLUSTER_IDS
FROM PersonLevelAsthmaCodingAggregation cod_agg
LEFT JOIN LatestMedicationDetails med
    ON cod_agg.PERSON_ID = med.PERSON_ID
WHERE
    cod_agg.HAS_ACTIVE_ASTHMA_DIAGNOSIS = TRUE
    AND med.LATEST_ASTHMA_MED_ORDER_DATE IS NOT NULL;
