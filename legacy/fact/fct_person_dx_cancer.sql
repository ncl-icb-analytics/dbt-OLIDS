CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_DX_CANCER (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age at achievement date

    IS_ON_CANCER_REGISTER BOOLEAN, -- Register flag: cancer diagnosis on/after 1 April 2003
    HAS_EPISODE_LAST_24M BOOLEAN, -- Flag: episode in last 24 months
    HAS_EPISODE_LAST_12M BOOLEAN, -- Flag: episode in last 12 months

    EARLIEST_CANCER_DATE DATE, -- Earliest cancer diagnosis date
    LATEST_CANCER_DATE DATE, -- Latest cancer diagnosis date

    ALL_CANCER_CONCEPT_CODES ARRAY, -- All cancer concept codes
    ALL_CANCER_CONCEPT_DISPLAYS ARRAY -- All cancer concept display terms
)
COMMENT = 'Fact table for Cancer register: patients with a diagnosis of cancer (excluding non-melanotic skin cancers) on or after 1 April 2003, plus flags for recent episodes.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH RegisterLogic AS (
    SELECT
        icd.*,
        -- Register logic: cancer diagnosis on/after 1 April 2003
        (
            icd.LATEST_CANCER_DATE >= DATE '2003-04-01'
        ) AS IS_ON_CANCER_REGISTER
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_CANCER_DETAILS icd
    WHERE icd.LATEST_CANCER_DATE IS NOT NULL -- Only include patients with a cancer diagnosis
)
SELECT
    rl.PERSON_ID,
    rl.SK_PATIENT_ID,
    rl.AGE,
    rl.IS_ON_CANCER_REGISTER,

    -- Recent episode flags
    (rl.LATEST_CANCER_DATE >= DATEADD(month, -24, CURRENT_DATE())) AS HAS_EPISODE_LAST_24M,
    (rl.LATEST_CANCER_DATE >= DATEADD(month, -12, CURRENT_DATE())) AS HAS_EPISODE_LAST_12M,

    rl.EARLIEST_CANCER_DATE,
    rl.LATEST_CANCER_DATE,

    rl.ALL_CANCER_CONCEPT_CODES,
    rl.ALL_CANCER_CONCEPT_DISPLAYS
FROM RegisterLogic rl
WHERE rl.IS_ON_CANCER_REGISTER = TRUE; -- Only keep patients on the register
