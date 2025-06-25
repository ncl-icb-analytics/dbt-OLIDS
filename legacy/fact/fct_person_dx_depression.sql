CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_DEPRESSION (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age at achievement date

    IS_ON_DEPRESSION_REGISTER BOOLEAN, -- Register flag: unresolved episode since 1 April 2006, age >= 18
    HAS_EPISODE_LAST_24M BOOLEAN, -- Flag: episode in last 24 months
    HAS_EPISODE_LAST_15M BOOLEAN, -- Flag: episode in last 15 months
    HAS_EPISODE_LAST_12M BOOLEAN, -- Flag: episode in last 12 months

    EARLIEST_DEPRESSION_DATE DATE, -- Earliest depression diagnosis date
    LATEST_DEPRESSION_DATE DATE, -- Latest depression diagnosis date
    LATEST_DEPRESSION_RESOLVED_DATE DATE, -- Latest resolved code date

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

WITH RegisterLogic AS (
    SELECT
        idd.*,
        -- Register logic: latest episode on/after 1 April 2006, unresolved (i.e. no resolution code after latest diagnosis), age >= 18
        (
            idd.LATEST_DEPRESSION_DATE >= DATE '2006-04-01'
            AND (idd.LATEST_DEPRESSION_RESOLVED_DATE IS NULL OR idd.LATEST_DEPRESSION_RESOLVED_DATE < idd.LATEST_DEPRESSION_DATE)
            AND idd.AGE >= 18
        ) AS IS_ON_DEPRESSION_REGISTER
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_DEPRESSION_DETAILS idd
    WHERE idd.LATEST_DEPRESSION_DATE IS NOT NULL -- Only include patients with a depression diagnosis
)
SELECT
    rl.PERSON_ID,
    rl.SK_PATIENT_ID,
    rl.AGE,
    rl.IS_ON_DEPRESSION_REGISTER,

    -- Recent episode flags
    (rl.LATEST_DEPRESSION_DATE >= DATEADD(month, -24, CURRENT_DATE())) AS HAS_EPISODE_LAST_24M,
    (rl.LATEST_DEPRESSION_DATE >= DATEADD(month, -15, CURRENT_DATE())) AS HAS_EPISODE_LAST_15M,
    (rl.LATEST_DEPRESSION_DATE >= DATEADD(month, -12, CURRENT_DATE())) AS HAS_EPISODE_LAST_12M,

    rl.EARLIEST_DEPRESSION_DATE,
    rl.LATEST_DEPRESSION_DATE,
    rl.LATEST_DEPRESSION_RESOLVED_DATE,

    rl.ALL_DEPRESSION_CONCEPT_CODES,
    rl.ALL_DEPRESSION_CONCEPT_DISPLAYS,
    rl.ALL_DEPRESSION_RESOLVED_CONCEPT_CODES,
    rl.ALL_DEPRESSION_RESOLVED_CONCEPT_DISPLAYS
FROM RegisterLogic rl
WHERE rl.IS_ON_DEPRESSION_REGISTER = TRUE; -- Only keep patients on the register
