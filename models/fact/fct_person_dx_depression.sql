CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_DEPRESSION (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age at achievement date

    IS_ON_DEPRESSION_REGISTER BOOLEAN, -- Register flag: unresolved episode since 1 April 2006, age >= 18
    HAS_EPISODE_LAST_24M BOOLEAN, -- Flag: episode in last 24 months
    HAS_EPISODE_LAST_15M BOOLEAN, -- Flag: episode in last 15 months
    HAS_EPISODE_LAST_12M BOOLEAN, -- Flag: episode in last 12 months

    LATEST_DEPRESSION_DATE DATE, -- Latest depression diagnosis date
    RAW_LATEST_DEPRESSION_RESOLVED_DATE DATE, -- Latest resolved code date

    ALL_DEPRESSION_CONCEPT_CODES ARRAY, -- All depression concept codes
    ALL_DEPRESSION_CONCEPT_DISPLAYS ARRAY, -- All depression concept display terms
    ALL_DEPRESSION_RESOLVED_CONCEPT_CODES ARRAY, -- All depression resolved concept codes
    ALL_DEPRESSION_RESOLVED_CONCEPT_DISPLAYS ARRAY -- All depression resolved concept display terms
)
COMMENT = 'Fact table for Depression register: patients aged 18+ with latest unresolved episode since 1 April 2006, plus flags for recent episodes.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT
    idd.PERSON_ID,
    idd.SK_PATIENT_ID,
    idd.AGE,

    -- Register logic: latest episode on/after 1 April 2006, unresolved, age >= 18
    (
        idd.LATEST_DEPRESSION_DATE >= DATE '2006-04-01'
        AND (idd.RAW_LATEST_DEPRESSION_RESOLVED_DATE IS NULL OR idd.RAW_LATEST_DEPRESSION_RESOLVED_DATE <= idd.LATEST_DEPRESSION_DATE)
        AND idd.AGE >= 18
    ) AS IS_ON_DEPRESSION_REGISTER,

    -- Recent episode flags
    (idd.LATEST_DEPRESSION_DATE >= DATEADD(month, -24, CURRENT_DATE())) AS HAS_EPISODE_LAST_24M,
    (idd.LATEST_DEPRESSION_DATE >= DATEADD(month, -15, CURRENT_DATE())) AS HAS_EPISODE_LAST_15M,
    (idd.LATEST_DEPRESSION_DATE >= DATEADD(month, -12, CURRENT_DATE())) AS HAS_EPISODE_LAST_12M,

    idd.LATEST_DEPRESSION_DATE,
    idd.RAW_LATEST_DEPRESSION_RESOLVED_DATE,

    idd.ALL_DEPRESSION_CONCEPT_CODES,
    idd.ALL_DEPRESSION_CONCEPT_DISPLAYS,
    idd.ALL_DEPRESSION_RESOLVED_CONCEPT_CODES,
    idd.ALL_DEPRESSION_RESOLVED_CONCEPT_DISPLAYS
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_DEPRESSION_DETAILS idd
WHERE idd.LATEST_DEPRESSION_DATE IS NOT NULL; -- Only include patients with a depression diagnosis 