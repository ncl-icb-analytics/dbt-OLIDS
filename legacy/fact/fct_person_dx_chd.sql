CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_CHD (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age at achievement date

    IS_ON_CHD_REGISTER BOOLEAN, -- Register flag: has CHD diagnosis
    HAS_EPISODE_LAST_24M BOOLEAN, -- Flag: episode in last 24 months
    HAS_EPISODE_LAST_12M BOOLEAN, -- Flag: episode in last 12 months

    EARLIEST_CHD_DATE DATE, -- Earliest CHD diagnosis date
    LATEST_CHD_DATE DATE, -- Latest CHD diagnosis date

    ALL_CHD_CONCEPT_CODES ARRAY, -- All CHD concept codes
    ALL_CHD_CONCEPT_DISPLAYS ARRAY -- All CHD concept display terms
)
COMMENT = 'Fact table for CHD register: patients with a coronary heart disease (CHD) diagnosis.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH BaseObservations AS (
    -- Get all CHD diagnoses
    SELECT 
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        AGE.AGE,
        O."clinical_effective_date"::DATE AS CLINICAL_EFFECTIVE_DATE,
        MC.CONCEPT_CODE,
        MC.CODE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
        ON O."patient_id" = P."id"
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE AS AGE
        ON PP."person_id" = AGE.PERSON_ID
    WHERE MC.CLUSTER_ID = 'CHD_COD'
),
AggregatedData AS (
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        AGE,
        MIN(CLINICAL_EFFECTIVE_DATE) AS EARLIEST_CHD_DATE,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_CHD_DATE,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) AS ALL_CHD_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CODE_DESCRIPTION) AS ALL_CHD_CONCEPT_DISPLAYS
    FROM BaseObservations
    GROUP BY PERSON_ID, SK_PATIENT_ID, AGE
)
SELECT
    ad.PERSON_ID,
    ad.SK_PATIENT_ID,
    ad.AGE,
    TRUE AS IS_ON_CHD_REGISTER, -- All patients with CHD diagnosis are on the register

    -- Recent episode flags
    (ad.LATEST_CHD_DATE >= DATEADD(month, -24, CURRENT_DATE())) AS HAS_EPISODE_LAST_24M,
    (ad.LATEST_CHD_DATE >= DATEADD(month, -12, CURRENT_DATE())) AS HAS_EPISODE_LAST_12M,

    ad.EARLIEST_CHD_DATE,
    ad.LATEST_CHD_DATE,

    ad.ALL_CHD_CONCEPT_CODES,
    ad.ALL_CHD_CONCEPT_DISPLAYS
FROM AggregatedData ad; 