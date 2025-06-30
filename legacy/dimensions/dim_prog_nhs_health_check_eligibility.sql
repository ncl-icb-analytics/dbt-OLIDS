CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_NHS_HEALTH_CHECK_ELIGIBILITY (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Current age of the person
    HAS_ANY_EXCLUDING_CONDITION BOOLEAN, -- Has any condition that would exclude from NHS Health Check
    IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK BOOLEAN -- Final eligibility flag
)
COMMENT = 'Dimension table containing ALL persons with their NHS Health Check eligibility status. For each person, shows their age and whether they have any excluding conditions (HAS_ANY_EXCLUDING_CONDITION). A person is eligible (IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK = TRUE) if they are aged 40-74 AND do not have any excluding conditions (CVD, diabetes, kidney disease, hypertension, AF, TIA, FH, heart failure, PAD, stroke) or are on statins. This table serves as the denominator for NHS Health Check programme analysis.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH PersonConditions AS (
    -- Get all relevant conditions from the fact table
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        MAX(CASE WHEN CONDITION_CODE IN ('CHD', 'DM', 'CKD', 'HTN', 'AF', 'STIA', 'FHYP', 'HF', 'PAD') THEN TRUE ELSE FALSE END) AS HAS_ANY_EXCLUDING_CONDITION
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.FCT_PERSON_LTC_SUMMARY
    GROUP BY PERSON_ID, SK_PATIENT_ID
),
PersonStatins AS (
    -- Get statin prescription status
    SELECT
        PERSON_ID,
        CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS HAS_STATIN_PRESCRIPTION
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_STATIN_ORDERS_ALL
    GROUP BY PERSON_ID
)
-- Final selection combining all eligibility criteria
SELECT
    pc.PERSON_ID,
    pc.SK_PATIENT_ID,
    age.AGE,
    -- Person has any excluding condition if they have any of the conditions or are on statins
    (COALESCE(pc.HAS_ANY_EXCLUDING_CONDITION, FALSE) OR COALESCE(ps.HAS_STATIN_PRESCRIPTION, FALSE)) AS HAS_ANY_EXCLUDING_CONDITION,
    -- Person is eligible if they are aged 40-74 and don't have any excluding conditions
    (
        age.AGE BETWEEN 40 AND 74 AND
        NOT (COALESCE(pc.HAS_ANY_EXCLUDING_CONDITION, FALSE) OR COALESCE(ps.HAS_STATIN_PRESCRIPTION, FALSE))
    ) AS IS_ELIGIBLE_FOR_NHS_HEALTH_CHECK
FROM PersonConditions pc
JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE age
    ON pc.PERSON_ID = age.PERSON_ID
LEFT JOIN PersonStatins ps
    ON pc.PERSON_ID = ps.PERSON_ID;
