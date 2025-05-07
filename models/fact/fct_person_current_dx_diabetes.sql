CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_DX_DIABETES (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (>= 17 for this table)
    IS_ON_DM_REGISTER BOOLEAN, -- Flag indicating if the person is currently on the diabetes register
    DIABETES_TYPE VARCHAR, -- Determined type of diabetes ('Type 1', 'Type 2', 'Unknown') or NULL if not on register
    EARLIEST_DM_DIAGNOSIS_DATE DATE, -- Earliest recorded date of any diabetes diagnosis
    EARLIEST_DMTYPE1_DIAGNOSIS_DATE DATE, -- Earliest recorded date of a Type 1 diabetes diagnosis
    EARLIEST_DMTYPE2_DIAGNOSIS_DATE DATE, -- Earliest recorded date of a Type 2 diabetes diagnosis
    LATEST_DM_DIAGNOSIS_DATE DATE, -- Latest recorded date of any diabetes diagnosis
    LATEST_DM_RESOLVED_DATE DATE, -- Latest recorded date of a diabetes resolved code
    LATEST_DMTYPE1_DIAGNOSIS_DATE DATE, -- Latest recorded date of a Type 1 diabetes diagnosis
    LATEST_DMTYPE2_DIAGNOSIS_DATE DATE, -- Latest recorded date of a Type 2 diabetes diagnosis
    ALL_DM_OBSERVATION_IDS ARRAY, -- Array of all observation IDs related to diabetes for the person
    ALL_DM_CONCEPT_CODES ARRAY, -- Array of all diabetes-related concept codes recorded for the person
    ALL_DM_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the diabetes-related concept codes
    ALL_DM_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (DM_COD, DMRES_COD, DMTYPE1_COD, DMTYPE2_COD)
)
COMMENT = 'Fact table identifying individuals aged 17 and over currently on the diabetes register, their diabetes type, and relevant diagnosis dates.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS

AS

WITH BaseObservationsAndClusters AS (
    -- Fetches all observation records related to diabetes diagnosis (DM_COD, DMTYPE1_COD, DMTYPE2_COD) 
    -- or diabetes resolution (DMRES_COD) by joining with the MAPPED_CONCEPTS table.
    -- Includes basic person identifiers and clinical effective dates.
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
        -- Select relevant fields from MAPPED_CONCEPTS
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY, -- Using CODE_DESCRIPTION from refset for consistency
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    -- Join to the pre-aggregated MAPPED_CONCEPTS table
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    -- Standard joins to get person and patient identifiers
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    -- Filter for the required diabetes-related cluster IDs directly from MAPPED_CONCEPTS
    WHERE MC.CLUSTER_ID IN ('DM_COD', 'DMRES_COD', 'DMTYPE1_COD', 'DMTYPE2_COD')
),
FilteredByAge AS (
    -- Filters the base diabetes-related observations to include only individuals aged 17 or older.
    SELECT
        boc.*,
        age.AGE
    FROM BaseObservationsAndClusters boc
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        ON boc.PERSON_ID = age.PERSON_ID -- Use the corrected join key
    WHERE age.AGE >= 17
),
PersonLevelAggregation AS (
    -- Aggregates diabetes-related information for each person aged 17+.
    -- Calculates earliest and latest dates for general diabetes, Type 1, Type 2, and resolved codes.
    -- Collects all associated observation details (IDs, codes, displays, cluster IDs) into arrays.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(AGE) as AGE,
        -- Find EARLIEST date for each relevant diagnosis cluster using conditional aggregation
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'DM_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_DM_DIAGNOSIS_DATE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'DMTYPE1_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_DMTYPE1_DIAGNOSIS_DATE,
        MIN(CASE WHEN SOURCE_CLUSTER_ID = 'DMTYPE2_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_DMTYPE2_DIAGNOSIS_DATE,
        -- Find LATEST date for each relevant cluster using conditional aggregation
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'DM_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_DM_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'DMRES_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_DM_RESOLVED_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'DMTYPE1_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_DMTYPE1_DIAGNOSIS_DATE,
        MAX(CASE WHEN SOURCE_CLUSTER_ID = 'DMTYPE2_COD' THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_DMTYPE2_DIAGNOSIS_DATE,
        -- Aggregate observation details
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_DM_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_DM_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_DM_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_DM_SOURCE_CLUSTER_IDS
    FROM FilteredByAge
    GROUP BY PERSON_ID
)
-- Final assembly of diabetes status and type for individuals on the register.
-- Determines IS_ON_DM_REGISTER based on diagnosis and resolution dates.
-- Derives DIABETES_TYPE based on the latest Type 1 or Type 2 codes for those on the register.
-- Filters to include only individuals currently on the diabetes register.
SELECT
    agg.PERSON_ID,
    agg.SK_PATIENT_ID,
    agg.AGE,
    -- Determine if currently on the diabetes register
    CASE
        WHEN agg.LATEST_DM_DIAGNOSIS_DATE IS NOT NULL AND
             (agg.LATEST_DM_RESOLVED_DATE IS NULL OR agg.LATEST_DM_DIAGNOSIS_DATE > agg.LATEST_DM_RESOLVED_DATE)
        THEN TRUE
        ELSE FALSE
    END AS IS_ON_DM_REGISTER,
    -- Determine Diabetes Type based on latest coding, only if on register
    CASE
        WHEN NOT IS_ON_DM_REGISTER THEN NULL -- Not applicable if not on register
        WHEN agg.LATEST_DMTYPE1_DIAGNOSIS_DATE IS NOT NULL AND
             (agg.LATEST_DMTYPE2_DIAGNOSIS_DATE IS NULL OR agg.LATEST_DMTYPE1_DIAGNOSIS_DATE >= agg.LATEST_DMTYPE2_DIAGNOSIS_DATE)
        THEN 'Type 1'
        WHEN agg.LATEST_DMTYPE2_DIAGNOSIS_DATE IS NOT NULL AND
             (agg.LATEST_DMTYPE1_DIAGNOSIS_DATE IS NULL OR agg.LATEST_DMTYPE2_DIAGNOSIS_DATE > agg.LATEST_DMTYPE1_DIAGNOSIS_DATE)
        THEN 'Type 2'
        ELSE 'Unknown' -- On register but no specific Type 1 or Type 2 code found, or types coded on same date (Type 1 priority)
    END AS DIABETES_TYPE,
    -- Include earliest dates in the final output
    agg.EARLIEST_DM_DIAGNOSIS_DATE,
    agg.EARLIEST_DMTYPE1_DIAGNOSIS_DATE,
    agg.EARLIEST_DMTYPE2_DIAGNOSIS_DATE,
    -- Include latest dates in the final output
    agg.LATEST_DM_DIAGNOSIS_DATE,
    agg.LATEST_DM_RESOLVED_DATE,
    agg.LATEST_DMTYPE1_DIAGNOSIS_DATE,
    agg.LATEST_DMTYPE2_DIAGNOSIS_DATE,
    -- Include aggregated details
    agg.ALL_DM_OBSERVATION_IDS,
    agg.ALL_DM_CONCEPT_CODES,
    agg.ALL_DM_CONCEPT_DISPLAYS,
    agg.ALL_DM_SOURCE_CLUSTER_IDS
FROM PersonLevelAggregation agg
WHERE IS_ON_DM_REGISTER = TRUE -- filter to only include patients where dm is not resolved 
