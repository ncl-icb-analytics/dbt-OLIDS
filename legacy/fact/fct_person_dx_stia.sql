CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_STIA (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_STIA_REGISTER BOOLEAN, -- Flag indicating if person is on the Stroke/TIA register
    EARLIEST_STROKE_DATE DATE, -- Earliest stroke diagnosis date
    LATEST_STROKE_DATE DATE, -- Latest stroke diagnosis date
    EARLIEST_TIA_DATE DATE, -- Earliest TIA diagnosis date
    LATEST_TIA_DATE DATE, -- Latest TIA diagnosis date
    EARLIEST_STIA_DATE DATE, -- Earliest of Stroke or TIA diagnosis date
    HAS_STROKE_DIAGNOSIS BOOLEAN, -- Flag indicating if the person has a stroke diagnosis
    HAS_TIA_DIAGNOSIS BOOLEAN, -- Flag indicating if the person has a TIA diagnosis
    ALL_STROKE_CONCEPT_CODES ARRAY, -- All stroke concept codes
    ALL_STROKE_CONCEPT_DISPLAYS ARRAY, -- All stroke concept display terms
    ALL_TIA_CONCEPT_CODES ARRAY, -- All TIA concept codes
    ALL_TIA_CONCEPT_DISPLAYS ARRAY -- All TIA concept display terms
)
COMMENT = 'Fact table identifying individuals with a diagnosis of Stroke (STRK_COD) or TIA (TIA_COD).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all Stroke and TIA diagnoses
    SELECT
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        AGE.AGE,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        MC.CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE AS AGE
        ON PP."person_id" = AGE.PERSON_ID
    WHERE MC.CLUSTER_ID IN ('STRK_COD', 'TIA_COD')
),
PersonLevelAggregation AS (
    -- Aggregate to one row per person with earliest/latest dates and concept arrays for Stroke and TIA
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        AGE,
        MIN(CASE WHEN CLUSTER_ID = 'STRK_COD' THEN CLINICAL_EFFECTIVE_DATE END) AS EARLIEST_STROKE_DATE,
        MAX(CASE WHEN CLUSTER_ID = 'STRK_COD' THEN CLINICAL_EFFECTIVE_DATE END) AS LATEST_STROKE_DATE,
        MIN(CASE WHEN CLUSTER_ID = 'TIA_COD' THEN CLINICAL_EFFECTIVE_DATE END) AS EARLIEST_TIA_DATE,
        MAX(CASE WHEN CLUSTER_ID = 'TIA_COD' THEN CLINICAL_EFFECTIVE_DATE END) AS LATEST_TIA_DATE,
        ARRAY_AGG(DISTINCT CASE WHEN CLUSTER_ID = 'STRK_COD' THEN CONCEPT_CODE END) AS ALL_STROKE_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN CLUSTER_ID = 'STRK_COD' THEN CODE_DESCRIPTION END) AS ALL_STROKE_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT CASE WHEN CLUSTER_ID = 'TIA_COD' THEN CONCEPT_CODE END) AS ALL_TIA_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CASE WHEN CLUSTER_ID = 'TIA_COD' THEN CODE_DESCRIPTION END) AS ALL_TIA_CONCEPT_DISPLAYS
    FROM BaseObservations
    GROUP BY PERSON_ID, SK_PATIENT_ID, AGE
)
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    pla.AGE,
    TRUE AS IS_ON_STIA_REGISTER, -- All patients in this table have either Stroke or TIA
    pla.EARLIEST_STROKE_DATE,
    pla.LATEST_STROKE_DATE,
    pla.EARLIEST_TIA_DATE,
    pla.LATEST_TIA_DATE,
    LEAST(COALESCE(pla.EARLIEST_STROKE_DATE, '9999-12-31'), COALESCE(pla.EARLIEST_TIA_DATE, '9999-12-31')) AS EARLIEST_STIA_DATE,
    (pla.EARLIEST_STROKE_DATE IS NOT NULL) AS HAS_STROKE_DIAGNOSIS,
    (pla.EARLIEST_TIA_DATE IS NOT NULL) AS HAS_TIA_DIAGNOSIS,
    FILTER(pla.ALL_STROKE_CONCEPT_CODES, x -> x IS NOT NULL) AS ALL_STROKE_CONCEPT_CODES, -- Filter out nulls from array_agg if no codes for type
    FILTER(pla.ALL_STROKE_CONCEPT_DISPLAYS, x -> x IS NOT NULL) AS ALL_STROKE_CONCEPT_DISPLAYS,
    FILTER(pla.ALL_TIA_CONCEPT_CODES, x -> x IS NOT NULL) AS ALL_TIA_CONCEPT_CODES,
    FILTER(pla.ALL_TIA_CONCEPT_DISPLAYS, x -> x IS NOT NULL) AS ALL_TIA_CONCEPT_DISPLAYS
FROM PersonLevelAggregation pla
WHERE pla.EARLIEST_STROKE_DATE IS NOT NULL OR pla.EARLIEST_TIA_DATE IS NOT NULL; -- Ensure patient has at least one diagnosis type
