CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_EPILEPSY (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (>= 18 for this table)
    IS_ON_EPIL_REGISTER BOOLEAN, -- Flag indicating if the person is currently on the epilepsy register
    -- Dates from Epilepsy Coding (EPIL_COD, EPILRES_COD)
    EARLIEST_EPIL_DIAGNOSIS_DATE DATE, -- Earliest recorded date of an epilepsy diagnosis code (EPIL_COD)
    LATEST_EPIL_DIAGNOSIS_DATE DATE, -- Latest recorded date of an epilepsy diagnosis code (EPIL_COD)
    LATEST_EPIL_RESOLVED_DATE DATE, -- Latest recorded date of an epilepsy resolved code (EPILRES_COD)
    -- Latest Medication Details
    LATEST_EPIL_MED_ORDER_DATE DATE, -- Date of the most recent epilepsy medication order
    LATEST_EPIL_MED_NAME VARCHAR, -- Name of the most recent epilepsy medication
    LATEST_EPIL_MED_DOSE VARCHAR, -- Dose of the most recent epilepsy medication
    LATEST_EPIL_MED_CONCEPT_CODE VARCHAR, -- Concept code of the most recent epilepsy medication
    LATEST_EPIL_MED_CONCEPT_DISPLAY VARCHAR, -- Display term for the most recent epilepsy medication
    RECENT_EPIL_MED_ORDER_COUNT NUMBER, -- Count of epilepsy medication orders in the last 6 months
    -- Aggregated details for traceability
    ALL_EPIL_CONCEPT_CODES ARRAY, -- Array of all EPIL_COD/EPILRES_COD concept codes recorded for the person
    ALL_EPIL_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the EPIL_COD/EPILRES_COD concept codes
    ALL_EPIL_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (EPIL_COD, EPILRES_COD)
)
COMMENT = 'Fact table identifying individuals aged 18 and over currently on the Epilepsy register. Requires both an active epilepsy diagnosis (latest EPIL_COD > latest EPILRES_COD) and a recent epilepsy medication order (within last 6 months).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservationsAndClusters AS (
    -- Fetches observation records related to epilepsy diagnosis (EPIL_COD) or resolution (EPILRES_COD).
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
    WHERE MC.CLUSTER_ID IN ('EPIL_COD', 'EPILRES_COD')
),
FilteredByAge AS (
    -- Filters the base epilepsy-related observations to include only individuals aged 18 or older.
    SELECT
        boc.*,
        age.AGE
    FROM BaseObservationsAndClusters boc
    JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE age
        ON boc.PERSON_ID = age.PERSON_ID
    WHERE age.AGE >= 18
),
PersonLevelEpilCodingAggregation AS (
    -- Aggregates epilepsy diagnosis and resolution code information for each person aged 18+.
    -- Calculates earliest/latest EPIL_COD dates and latest EPILRES_COD date.
    -- Determines IS_ON_EPIL_REGISTER: TRUE if there's an active EPIL_COD (latest EPIL_COD > latest EPILRES_COD, or no EPILRES_COD).
    -- Collects all associated concept details (codes, displays, cluster IDs) into arrays.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'EPIL_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_EPIL_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'EPIL_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_EPIL_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'EPILRES_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_EPIL_RESOLVED_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_EPIL_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_EPIL_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_EPIL_SOURCE_CLUSTER_IDS,
        CASE
            WHEN LATEST_EPIL_DIAGNOSIS_DATE IS NOT NULL AND
                 (LATEST_EPIL_RESOLVED_DATE IS NULL OR LATEST_EPIL_DIAGNOSIS_DATE > LATEST_EPIL_RESOLVED_DATE)
            THEN TRUE
            ELSE FALSE
        END AS HAS_ACTIVE_EPIL_DIAGNOSIS
    FROM FilteredByAge
    GROUP BY PERSON_ID
),
LatestMedicationDetails AS (
    -- Gets the most recent epilepsy medication order for each person
    SELECT
        PERSON_ID,
        MAX(ORDER_DATE) as LATEST_EPIL_MED_ORDER_DATE,
        ANY_VALUE(ORDER_MEDICATION_NAME) as LATEST_EPIL_MED_NAME,
        ANY_VALUE(ORDER_DOSE) as LATEST_EPIL_MED_DOSE,
        ANY_VALUE(MAPPED_CONCEPT_CODE) as LATEST_EPIL_MED_CONCEPT_CODE,
        ANY_VALUE(MAPPED_CONCEPT_DISPLAY) as LATEST_EPIL_MED_CONCEPT_DISPLAY,
        ANY_VALUE(RECENT_ORDER_COUNT) as RECENT_EPIL_MED_ORDER_COUNT
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_EPILEPSY_ORDERS_6M
    GROUP BY PERSON_ID
)
-- Final selection: Combines epilepsy coding status with medication details.
-- Filters to include only individuals who meet both criteria:
-- 1. Have an active epilepsy diagnosis (latest EPIL_COD > latest EPILRES_COD)
-- 2. Have a recent epilepsy medication order (within last 6 months)
SELECT
    cod_agg.PERSON_ID,
    cod_agg.SK_PATIENT_ID,
    cod_agg.AGE,
    CASE
        WHEN cod_agg.HAS_ACTIVE_EPIL_DIAGNOSIS = TRUE
        AND med.LATEST_EPIL_MED_ORDER_DATE IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS IS_ON_EPIL_REGISTER,
    -- Coding Dates
    cod_agg.EARLIEST_EPIL_DIAGNOSIS_DATE,
    cod_agg.LATEST_EPIL_DIAGNOSIS_DATE,
    cod_agg.LATEST_EPIL_RESOLVED_DATE,
    -- Latest Medication Details
    med.LATEST_EPIL_MED_ORDER_DATE,
    med.LATEST_EPIL_MED_NAME,
    med.LATEST_EPIL_MED_DOSE,
    med.LATEST_EPIL_MED_CONCEPT_CODE,
    med.LATEST_EPIL_MED_CONCEPT_DISPLAY,
    med.RECENT_EPIL_MED_ORDER_COUNT,
    -- Coding Traceability
    cod_agg.ALL_EPIL_CONCEPT_CODES,
    cod_agg.ALL_EPIL_CONCEPT_DISPLAYS,
    cod_agg.ALL_EPIL_SOURCE_CLUSTER_IDS
FROM PersonLevelEpilCodingAggregation cod_agg
LEFT JOIN LatestMedicationDetails med
    ON cod_agg.PERSON_ID = med.PERSON_ID
WHERE
    cod_agg.HAS_ACTIVE_EPIL_DIAGNOSIS = TRUE
    AND med.LATEST_EPIL_MED_ORDER_DATE IS NOT NULL;
