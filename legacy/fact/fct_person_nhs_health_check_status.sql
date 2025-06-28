CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_NHS_HEALTH_CHECK_STATUS (
    -- Identifiers
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    -- Eligibility Details
    AGE NUMBER, -- Current age of the person
    HAS_ANY_EXCLUDING_CONDITION BOOLEAN, -- Has any condition that would exclude from NHS Health Check
    IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK BOOLEAN, -- Whether person meets eligibility criteria
    -- Health Check Details
    LATEST_HEALTH_CHECK_DATE DATE, -- Date of the most recent NHS Health Check
    DAYS_SINCE_LAST_HEALTH_CHECK NUMBER, -- Number of days since last health check (NULL if never had one)
    DUE_NHS_HEALTH_CHECK BOOLEAN -- Whether person is due an NHS Health Check (eligible AND (never had one OR last one > 5 years ago))
)
COMMENT = 'Fact table containing ALL persons with their NHS Health Check status. For each person aged 40-74, shows their eligibility (IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK) and whether they are due a check (DUE_NHS_HEALTH_CHECK). A person is due a check if they are eligible AND (have never had one OR their last check was > 5 years ago). Ineligible persons will always have DUE_NHS_HEALTH_CHECK = FALSE.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH LatestHealthCheck AS (
    -- Get the most recent NHS Health Check for each person
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE AS LATEST_HEALTH_CHECK_DATE,
        DATEDIFF(day, CLINICAL_EFFECTIVE_DATE, CURRENT_DATE()) AS DAYS_SINCE_LAST_HEALTH_CHECK
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_NHS_HEALTH_CHECK_LATEST
)
-- Final selection combining eligibility and due status
SELECT
    d.PERSON_ID,
    d.SK_PATIENT_ID,
    d.AGE,
    d.HAS_ANY_EXCLUDING_CONDITION,
    d.IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK,
    hc.LATEST_HEALTH_CHECK_DATE,
    hc.DAYS_SINCE_LAST_HEALTH_CHECK,
    -- Person is due a health check if they are eligible AND (never had one OR last one > 5 years ago)
    (
        d.IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK AND
        (hc.DAYS_SINCE_LAST_HEALTH_CHECK IS NULL OR hc.DAYS_SINCE_LAST_HEALTH_CHECK > 1825)
    ) AS DUE_NHS_HEALTH_CHECK
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_NHS_HEALTH_CHECK_ELIGIBILITY d
LEFT JOIN LatestHealthCheck hc
    ON d.PERSON_ID = hc.PERSON_ID;
