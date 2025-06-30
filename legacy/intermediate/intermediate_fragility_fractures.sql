CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_FRAGILITY_FRACTURES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the fracture
    CONCEPT_CODE VARCHAR, -- The concept code for the fracture
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    FRACTURE_SITE VARCHAR, -- The anatomical site of the fracture (e.g., hip, wrist, spine)
    IS_FRAGILITY_FRACTURE BOOLEAN, -- Flag indicating if this is a fragility fracture
    EARLIEST_FRACTURE_DATE DATE, -- Earliest fragility fracture date after April 2012
    LATEST_FRACTURE_DATE DATE, -- Latest fragility fracture date after April 2012
    ALL_FRACTURE_CONCEPT_CODES ARRAY, -- All fragility fracture concept codes
    ALL_FRACTURE_CONCEPT_DISPLAYS ARRAY, -- All fragility fracture concept display terms
    ALL_FRACTURE_SITES ARRAY -- All unique fracture sites for the person
)
COMMENT = 'Intermediate table containing fragility fractures, including fracture sites and dates. Only includes fractures after April 2012.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION,
        -- Extract fracture site from code description
        CASE
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%hip%' THEN 'Hip'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%wrist%' OR LOWER(MC.CODE_DESCRIPTION) LIKE '%radius%' THEN 'Wrist'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%spine%' OR LOWER(MC.CODE_DESCRIPTION) LIKE '%vertebra%' THEN 'Spine'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%humerus%' OR LOWER(MC.CODE_DESCRIPTION) LIKE '%shoulder%' THEN 'Humerus'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%pelvis%' THEN 'Pelvis'
            WHEN LOWER(MC.CODE_DESCRIPTION) LIKE '%femur%' THEN 'Femur'
            ELSE 'Other'
        END AS FRACTURE_SITE
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    WHERE MC.CLUSTER_ID = 'FF_COD'
),
PersonDates AS (
    SELECT
        bo.*,
        -- Flag fragility fractures
        TRUE AS IS_FRAGILITY_FRACTURE,
        -- Get earliest and latest fracture dates (post-April 2012 only)
        MIN(CASE WHEN CLINICAL_EFFECTIVE_DATE >= '2012-04-01' THEN CLINICAL_EFFECTIVE_DATE END)
            OVER (PARTITION BY PERSON_ID) AS EARLIEST_FRACTURE_DATE,
        MAX(CASE WHEN CLINICAL_EFFECTIVE_DATE >= '2012-04-01' THEN CLINICAL_EFFECTIVE_DATE END)
            OVER (PARTITION BY PERSON_ID) AS LATEST_FRACTURE_DATE
    FROM BaseObservations bo
),
PersonLevelCodingAggregation AS (
    -- Aggregate all concept codes, displays, and fracture sites into arrays
    SELECT
        PERSON_ID,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_FRACTURE_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_FRACTURE_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT FRACTURE_SITE) AS ALL_FRACTURE_SITES
    FROM BaseObservations
    WHERE CLINICAL_EFFECTIVE_DATE >= '2012-04-01'
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
    pd.FRACTURE_SITE,
    pd.IS_FRAGILITY_FRACTURE,
    pd.EARLIEST_FRACTURE_DATE,
    pd.LATEST_FRACTURE_DATE,
    c.ALL_FRACTURE_CONCEPT_CODES,
    c.ALL_FRACTURE_CONCEPT_DISPLAYS,
    c.ALL_FRACTURE_SITES
FROM PersonDates pd
LEFT JOIN PersonLevelCodingAggregation c
    ON pd.PERSON_ID = c.PERSON_ID
WHERE pd.CLINICAL_EFFECTIVE_DATE >= '2012-04-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY pd.PERSON_ID ORDER BY pd.CLINICAL_EFFECTIVE_DATE) = 1;
