-- Creates a dynamic table identifying patients currently coded as having Dementia (DEM).
-- Inclusion criteria: Patients with a DEM_COD.
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_DEMENTIA (
    -- Identifiers
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    -- Dementia Coding Dates & Status
    EARLIEST_DEM_DIAGNOSIS_DATE DATE, -- Earliest recorded date of a dementia diagnosis code (DEM_COD)
    LATEST_DEM_DIAGNOSIS_DATE DATE, -- Latest recorded date of a dementia diagnosis code (DEM_COD)
    IS_ON_DEM_REGISTER_CALC BOOLEAN, -- Flag: TRUE if DEM_COD is present
    -- Coding Traceability for Dementia Diagnosis
    ALL_DEM_OBSERVATION_IDS ARRAY, -- Array of all observation IDs related to DEM_COD for the person
    ALL_DEM_CONCEPT_CODES ARRAY, -- Array of all DEM_COD concept codes recorded for the person
    ALL_DEM_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the DEM_COD concept codes
    ALL_DEM_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (DEM_COD)
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Identifies patients currently coded as having Dementia (presence of DEM_COD).'
AS
WITH BaseObservationsAndClusters AS (
    -- Fetches observation records related to Dementia diagnosis (DEM_COD).
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
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID IN ('DEM_COD')
),
PersonLevelDEMCodingAggregation AS (
    -- Aggregates dementia diagnosis code information for each person.
    -- Calculates earliest/latest DEM_COD dates.
    -- Determines IS_ON_DEM_REGISTER_CALC: TRUE if there's any DEM_COD.
    -- Collects all associated observation details (IDs, codes, displays, cluster IDs) into arrays.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'DEM_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_DEM_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'DEM_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_DEM_DIAGNOSIS_DATE,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_DEM_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_DEM_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_DEM_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_DEM_SOURCE_CLUSTER_IDS,
        CASE
            WHEN LATEST_DEM_DIAGNOSIS_DATE IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS IS_ON_DEM_REGISTER_CALC
    FROM BaseObservationsAndClusters
    GROUP BY PERSON_ID
)
-- Final selection: Filters for individuals on the calculated DEM register.
SELECT
    dem_agg.PERSON_ID,
    dem_agg.SK_PATIENT_ID,
    -- Coding Dates & Status
    dem_agg.EARLIEST_DEM_DIAGNOSIS_DATE,
    dem_agg.LATEST_DEM_DIAGNOSIS_DATE,
    dem_agg.IS_ON_DEM_REGISTER_CALC,
    -- Coding Traceability
    dem_agg.ALL_DEM_OBSERVATION_IDS,
    dem_agg.ALL_DEM_CONCEPT_CODES,
    dem_agg.ALL_DEM_CONCEPT_DISPLAYS,
    dem_agg.ALL_DEM_SOURCE_CLUSTER_IDS
FROM PersonLevelDEMCodingAggregation dem_agg
WHERE dem_agg.IS_ON_DEM_REGISTER_CALC = TRUE; 