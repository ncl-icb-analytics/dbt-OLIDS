CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_CKD_LAB_INFERENCE (
    PERSON_ID VARCHAR,
    SK_PATIENT_ID VARCHAR,
    -- Latest Lab Info
    LATEST_EGFR_VALUE NUMBER,
    LATEST_EGFR_DATE DATE,
    LATEST_ACR_VALUE NUMBER,
    LATEST_ACR_DATE DATE,
    LATEST_EGFR_STAGE VARCHAR, -- G1-G5 based on latest eGFR
    LATEST_ACR_STAGE VARCHAR, -- A1-A3 based on latest ACR
    LATEST_CKD_STAGE_INFERRED VARCHAR, -- Combined G/A stage (e.g., 'G3a A2') from latest labs, indicates severity if CKD present
    LATEST_LABS_MEET_CKD_CRITERIA BOOLEAN, -- TRUE if latest G/A stage indicates CKD presence (eGFR<60 OR eGFR>=60 with ACR>=3)
    -- Confirmation Flags (based on persistence over 90 days within a ~2 year window)
    HAS_CONFIRMED_LOW_EGFR BOOLEAN, -- TRUE if two eGFR < 60 results found >= 90 days AND <= 730 days apart
    HAS_CONFIRMED_HIGH_ACR BOOLEAN, -- TRUE if two ACR >= 3 results found >= 90 days AND <= 730 days apart
    HAS_CONFIRMED_CKD_BY_LABS BOOLEAN -- TRUE if either HAS_CONFIRMED_LOW_EGFR or HAS_CONFIRMED_HIGH_ACR is TRUE
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS -- Use appropriate warehouse for your environment
AS

WITH AllEGFRWithStage AS (
    -- Calculate G stage for every eGFR result and flag if < 60
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        RESULT_VALUE,
        CASE
            WHEN RESULT_VALUE IS NULL THEN NULL
            WHEN RESULT_VALUE >= 90 THEN 'G1'
            WHEN RESULT_VALUE BETWEEN 60 AND 89 THEN 'G2'
            WHEN RESULT_VALUE BETWEEN 45 AND 59 THEN 'G3a'
            WHEN RESULT_VALUE BETWEEN 30 AND 44 THEN 'G3b'
            WHEN RESULT_VALUE BETWEEN 15 AND 29 THEN 'G4'
            WHEN RESULT_VALUE < 15 THEN 'G5'
            ELSE NULL
        END AS EGFR_STAGE,
        -- Flag if this specific result is low (< 60)
        (RESULT_VALUE < 60) AS IS_LOW_EGFR
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_EGFR_ALL -- Use the ALL table containing all historical results
),
AllACRWithStage AS (
    -- Calculate A stage for every ACR result and flag if >= 3
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        RESULT_VALUE,
         CASE
            WHEN RESULT_VALUE IS NULL THEN NULL
            WHEN RESULT_VALUE < 3 THEN 'A1'
            WHEN RESULT_VALUE BETWEEN 3 AND 30 THEN 'A2'
            WHEN RESULT_VALUE > 30 THEN 'A3'
            ELSE NULL
        END AS ACR_STAGE,
        -- Flag if this specific result is high (>= 3)
        (RESULT_VALUE >= 3) AS IS_HIGH_ACR
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_ALL -- Use the ALL table containing all historical results
),
EGFRConfirmationCheck AS (
    -- For each low eGFR result, find the date of the immediately preceding low eGFR result
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE,
        IS_LOW_EGFR, -- Will always be TRUE due to WHERE clause below
        -- Look back for the date of the previous low eGFR result for the same person
        LAG(CASE WHEN IS_LOW_EGFR THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END, 1) IGNORE NULLS
            OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE ASC) AS PREV_LOW_EGFR_DATE
    FROM AllEGFRWithStage
    WHERE IS_LOW_EGFR -- Only consider rows where eGFR is already low
),
ACRConfirmationCheck AS (
    -- For each high ACR result, find the date of the immediately preceding high ACR result
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE,
        IS_HIGH_ACR, -- Will always be TRUE due to WHERE clause below
        -- Look back for the date of the previous high ACR result for the same person
        LAG(CASE WHEN IS_HIGH_ACR THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END, 1) IGNORE NULLS
            OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE ASC) AS PREV_HIGH_ACR_DATE
    FROM AllACRWithStage
    WHERE IS_HIGH_ACR -- Only consider rows where ACR is already high
),
PersonLevelConfirmation AS (
    -- Determine per person if criteria for confirmed low eGFR are met
    SELECT
        PERSON_ID,
        -- Check if *any* low eGFR result had a previous low eGFR result >= 90 days AND <= 730 days before it
        MAX(CASE
                WHEN PREV_LOW_EGFR_DATE IS NOT NULL -- Ensure there *was* a previous low result
                 AND DATEDIFF(day, PREV_LOW_EGFR_DATE, CLINICAL_EFFECTIVE_DATE) >= 90 -- Check persistence > 3 months
                 AND DATEDIFF(day, PREV_LOW_EGFR_DATE, CLINICAL_EFFECTIVE_DATE) <= 730 -- Check max ~2 year window
                THEN 1
                ELSE 0
            END) = 1 AS HAS_CONFIRMED_LOW_EGFR
    FROM EGFRConfirmationCheck
    GROUP BY PERSON_ID
),
PersonLevelACRConfirmation AS (
    -- Determine per person if criteria for confirmed high ACR are met
    SELECT
        PERSON_ID,
        -- Check if *any* high ACR result had a previous high ACR result >= 90 days AND <= 730 days before it
        MAX(CASE
                WHEN PREV_HIGH_ACR_DATE IS NOT NULL -- Ensure there *was* a previous high result
                 AND DATEDIFF(day, PREV_HIGH_ACR_DATE, CLINICAL_EFFECTIVE_DATE) >= 90 -- Check persistence > 3 months
                 AND DATEDIFF(day, PREV_HIGH_ACR_DATE, CLINICAL_EFFECTIVE_DATE) <= 730 -- Check max ~2 year window
                THEN 1
                ELSE 0
            END) = 1 AS HAS_CONFIRMED_HIGH_ACR
    FROM ACRConfirmationCheck
    GROUP BY PERSON_ID
),
LatestLabs AS (
    -- Get the single latest EGFR and ACR result for each person
    SELECT
        COALESCE(e.PERSON_ID, a.PERSON_ID) as PERSON_ID,
        COALESCE(e.SK_PATIENT_ID, a.SK_PATIENT_ID) as SK_PATIENT_ID, -- Assuming SK_PATIENT_ID is consistent
        e.RESULT_VALUE AS LATEST_EGFR_VALUE,
        e.CLINICAL_EFFECTIVE_DATE AS LATEST_EGFR_DATE,
        a.RESULT_VALUE AS LATEST_ACR_VALUE,
        a.CLINICAL_EFFECTIVE_DATE AS LATEST_ACR_DATE,
        -- Recalculate latest stages based on these specific latest values
        CASE
            WHEN e.RESULT_VALUE IS NULL THEN NULL
            WHEN e.RESULT_VALUE >= 90 THEN 'G1'
            WHEN e.RESULT_VALUE BETWEEN 60 AND 89 THEN 'G2'
            WHEN e.RESULT_VALUE BETWEEN 45 AND 59 THEN 'G3a'
            WHEN e.RESULT_VALUE BETWEEN 30 AND 44 THEN 'G3b'
            WHEN e.RESULT_VALUE BETWEEN 15 AND 29 THEN 'G4'
            WHEN e.RESULT_VALUE < 15 THEN 'G5'
            ELSE NULL
        END AS LATEST_EGFR_STAGE,
        CASE
            WHEN a.RESULT_VALUE IS NULL THEN NULL
            WHEN a.RESULT_VALUE < 3 THEN 'A1'
            WHEN a.RESULT_VALUE BETWEEN 3 AND 30 THEN 'A2'
            WHEN a.RESULT_VALUE > 30 THEN 'A3'
            ELSE NULL
        END AS LATEST_ACR_STAGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_EGFR_LATEST e -- Uses the LATEST eGFR table
    FULL OUTER JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_LATEST a -- Uses the LATEST ACR table
        ON e.PERSON_ID = a.PERSON_ID
)
-- Final assembly: Combine latest lab info with aggregated confirmation flags
SELECT
    ll.PERSON_ID,
    ll.SK_PATIENT_ID,
    ll.LATEST_EGFR_VALUE,
    ll.LATEST_EGFR_DATE,
    ll.LATEST_ACR_VALUE,
    ll.LATEST_ACR_DATE,
    ll.LATEST_EGFR_STAGE,
    ll.LATEST_ACR_STAGE,
    -- Combine latest G and A stages into inferred CKD stage string
    CASE
        WHEN ll.LATEST_EGFR_STAGE IS NOT NULL AND ll.LATEST_ACR_STAGE IS NOT NULL THEN ll.LATEST_EGFR_STAGE || ' ' || ll.LATEST_ACR_STAGE
        WHEN ll.LATEST_EGFR_STAGE IS NOT NULL THEN ll.LATEST_EGFR_STAGE || ' A?' -- Indicate missing ACR stage
        WHEN ll.LATEST_ACR_STAGE IS NOT NULL THEN 'G? ' || ll.LATEST_ACR_STAGE -- Indicate missing eGFR stage
        ELSE NULL
    END AS LATEST_CKD_STAGE_INFERRED,
    -- Determine if the *latest* lab results alone meet criteria for CKD diagnosis
    CASE
        WHEN ll.LATEST_EGFR_STAGE IN ('G3a', 'G3b', 'G4', 'G5') THEN TRUE -- eGFR < 60 always indicates CKD
        WHEN ll.LATEST_EGFR_STAGE IN ('G1', 'G2') AND ll.LATEST_ACR_STAGE IN ('A2', 'A3') THEN TRUE -- eGFR >= 60 needs ACR >= 3
        ELSE FALSE
    END AS LATEST_LABS_MEET_CKD_CRITERIA,
    -- Bring in eGFR confirmation flag (default to FALSE if no confirmation found)
    COALESCE(plc.HAS_CONFIRMED_LOW_EGFR, FALSE) AS HAS_CONFIRMED_LOW_EGFR,
    -- Bring in ACR confirmation flag (default to FALSE if no confirmation found)
    COALESCE(plac.HAS_CONFIRMED_HIGH_ACR, FALSE) AS HAS_CONFIRMED_HIGH_ACR,
    -- Determine overall lab confirmation status
    (COALESCE(plc.HAS_CONFIRMED_LOW_EGFR, FALSE) OR COALESCE(plac.HAS_CONFIRMED_HIGH_ACR, FALSE)) AS HAS_CONFIRMED_CKD_BY_LABS
FROM LatestLabs ll
-- Left join confirmation results back to the latest lab results for each person
LEFT JOIN PersonLevelConfirmation plc ON ll.PERSON_ID = plc.PERSON_ID
LEFT JOIN PersonLevelACRConfirmation plac ON ll.PERSON_ID = plac.PERSON_ID;