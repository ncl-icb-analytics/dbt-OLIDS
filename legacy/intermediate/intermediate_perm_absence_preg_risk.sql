CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PERM_ABSENCE_PREG_RISK (
    PERSON_ID VARCHAR COMMENT 'Unique identifier for the person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    AGE NUMBER COMMENT 'Current age of the person from DIM_PERSON_AGE',
    HAS_PERM_ABS_PREG_RISK_EVENT BOOLEAN COMMENT 'Always TRUE, indicating at least one relevant event meeting criteria',
    EARLIEST_PERM_ABS_PREG_RISK_EVENT_DATE DATE COMMENT 'Earliest date of any relevant event for the person',
    LATEST_PERM_ABS_PREG_RISK_EVENT_DATE DATE COMMENT 'Latest date of any relevant event for the person',
    ALL_PERM_ABS_PREG_RISK_CONCEPT_CODES ARRAY COMMENT 'Array of all distinct relevant medical concept codes found',
    ALL_PERM_ABS_PREG_RISK_CONCEPT_DISPLAYS ARRAY COMMENT 'Array of display terms for the concept codes (from MAPPED_CONCEPTS.CODE_DESCRIPTION)',
    ALL_PERM_ABS_PREG_RISK_SOURCE_CLUSTER_IDS ARRAY COMMENT 'Array of all source cluster IDs associated with permanent absence of pregnancy risk events'
)
COMMENT = 'Intermediate table aggregating events related to Permanent Absence of Pregnancy Risk for each person. It uses MAPPED_CONCEPTS, OBSERVATION, and codes defined under the \'PREGRISK\' category in VALPROATE_PROG_CODES. Includes current age.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePermAbsPregRiskObservations AS (
    -- Selects observations related to Permanent Absence of Pregnancy Risk.
    -- LOOKBACK_YEARS_OFFSET is NULL for all PREGRISK codes, so all historical records are considered.
    SELECT
        PP."person_id" AS PERSON_ID,
        PAT."sk_patient_id" AS SK_PATIENT_ID,
        AGE_DIM.AGE AS CURRENT_AGE,
        O."id" AS OBSERVATION_ID,
        O."clinical_effective_date"::DATE AS EVENT_DATE,
        MC.CONCEPT_CODE AS CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS CONCEPT_DISPLAY,
        VPC.CODE_CATEGORY AS CODE_CATEGORY, -- This will be 'PREGRISK'
        MC.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.CODESETS.VALPROATE_PROG_CODES AS VPC
        ON MC.CONCEPT_CODE = VPC.CODE
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS PAT
        ON PP."patient_id" = PAT."id"
    JOIN
        DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE AS AGE_DIM
        ON PP."person_id" = AGE_DIM.PERSON_ID
    WHERE
        VPC.CODE_CATEGORY = 'PREGRISK'
        AND (
            VPC.LOOKBACK_YEARS_OFFSET IS NULL OR
            O."clinical_effective_date"::DATE >= DATEADD(YEAR, VPC.LOOKBACK_YEARS_OFFSET, CURRENT_DATE())
        )
),
PersonLevelPermAbsPregRiskAggregation AS (
    -- Aggregates Permanent Absence of Pregnancy Risk information for each person.
    SELECT
        PERSON_ID,
        ANY_VALUE(SK_PATIENT_ID) as SK_PATIENT_ID,
        ANY_VALUE(CURRENT_AGE) as AGE,
        MIN(EVENT_DATE) AS EARLIEST_PERM_ABS_PREG_RISK_EVENT_DATE,
        MAX(EVENT_DATE) AS LATEST_PERM_ABS_PREG_RISK_EVENT_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_PERM_ABS_PREG_RISK_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_PERM_ABS_PREG_RISK_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_PERM_ABS_PREG_RISK_SOURCE_CLUSTER_IDS
    FROM BasePermAbsPregRiskObservations
    GROUP BY PERSON_ID
)
SELECT
    pla.PERSON_ID,
    pla.SK_PATIENT_ID,
    pla.AGE,
    TRUE AS HAS_PERM_ABS_PREG_RISK_EVENT,
    pla.EARLIEST_PERM_ABS_PREG_RISK_EVENT_DATE,
    pla.LATEST_PERM_ABS_PREG_RISK_EVENT_DATE,
    pla.ALL_PERM_ABS_PREG_RISK_CONCEPT_CODES,
    pla.ALL_PERM_ABS_PREG_RISK_CONCEPT_DISPLAYS,
    pla.ALL_PERM_ABS_PREG_RISK_SOURCE_CLUSTER_IDS
FROM PersonLevelPermAbsPregRiskAggregation pla;
