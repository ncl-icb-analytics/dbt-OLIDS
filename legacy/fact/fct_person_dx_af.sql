 CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_AF (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_AF_REGISTER BOOLEAN, -- Flag indicating if the person is currently on the AF register
    -- Dates from AF Coding (AFIB_COD, AFIBRES_COD)
    EARLIEST_AF_DIAGNOSIS_DATE DATE, -- Earliest recorded date of an AF diagnosis code (AFIB_COD)
    LATEST_AF_DIAGNOSIS_DATE DATE, -- Latest recorded date of an AF diagnosis code (AFIB_COD)
    LATEST_AF_RESOLVED_DATE DATE, -- Latest recorded date of an AF resolved code (AFIBRES_COD)
    -- Aggregated details for traceability
    ALL_AF_CONCEPT_CODES ARRAY, -- Array of all AFIB_COD/AFIBRES_COD concept codes recorded for the person
    ALL_AF_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for the AFIB_COD/AFIBRES_COD concept codes
    ALL_AF_SOURCE_CLUSTER_IDS ARRAY -- Array of source cluster IDs (AFIB_COD, AFIBRES_COD)
)
COMMENT = 'Fact table identifying individuals currently on the Atrial Fibrillation register. Requires an active AF diagnosis (latest AFIB_COD > latest AFIBRES_COD).'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservationsAndClusters AS (
    -- Fetches observation records related to AF diagnosis (AFIB_COD) or resolution (AFIBRES_COD).
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
    WHERE MC.CLUSTER_ID IN ('AFIB_COD', 'AFIBRES_COD')
),
PersonLevelAFCodingAggregation AS (
    -- Aggregates AF diagnosis and resolution code information for each person.
    -- Calculates earliest/latest AFIB_COD dates and latest AFIBRES_COD date.
    -- Determines IS_ON_AF_REGISTER: TRUE if there's an active AFIB_COD (latest AFIB_COD > latest AFIBRES_COD, or no AFIBRES_COD).
    -- Collects all associated concept details (codes, displays, cluster IDs) into arrays.
    SELECT
        boc.PERSON_ID,
        ANY_VALUE(boc.SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(age.AGE) as AGE,
        MIN(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIB_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS EARLIEST_AF_DIAGNOSIS_DATE,
        MAX(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIB_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_AF_DIAGNOSIS_DATE,
        MAX(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIBRES_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END) AS LATEST_AF_RESOLVED_DATE,
        ARRAY_AGG(DISTINCT boc.CONCEPT_CODE) WITHIN GROUP (ORDER BY boc.CONCEPT_CODE) AS ALL_AF_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT boc.CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY boc.CONCEPT_DISPLAY) AS ALL_AF_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT boc.SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY boc.SOURCE_CLUSTER_ID) AS ALL_AF_SOURCE_CLUSTER_IDS,
        CASE
            WHEN MAX(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIB_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END) IS NOT NULL AND
                 (MAX(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIBRES_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END) IS NULL
                  OR MAX(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIB_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END) >
                     MAX(CASE WHEN boc.SOURCE_CLUSTER_ID = 'AFIBRES_COD' THEN boc.CLINICAL_EFFECTIVE_DATE ELSE NULL END))
            THEN TRUE
            ELSE FALSE
        END AS HAS_ACTIVE_AF_DIAGNOSIS
    FROM BaseObservationsAndClusters boc
    LEFT JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE age
        ON boc.PERSON_ID = age.PERSON_ID
    GROUP BY boc.PERSON_ID
)
-- Final selection: Includes all individuals with an active AF diagnosis
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    HAS_ACTIVE_AF_DIAGNOSIS AS IS_ON_AF_REGISTER,
    -- Coding Dates
    EARLIEST_AF_DIAGNOSIS_DATE,
    LATEST_AF_DIAGNOSIS_DATE,
    LATEST_AF_RESOLVED_DATE,
    -- Coding Traceability
    ALL_AF_CONCEPT_CODES,
    ALL_AF_CONCEPT_DISPLAYS,
    ALL_AF_SOURCE_CLUSTER_IDS
FROM PersonLevelAFCodingAggregation
WHERE HAS_ACTIVE_AF_DIAGNOSIS = TRUE;
