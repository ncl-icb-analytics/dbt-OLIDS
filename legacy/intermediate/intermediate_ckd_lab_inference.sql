CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_CKD_LAB_INFERENCE (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    -- Latest Lab Info
    LATEST_EGFR_VALUE NUMBER, -- The numeric value of the most recent eGFR test
    LATEST_EGFR_DATE DATE, -- The date of the most recent eGFR test
    LATEST_ACR_VALUE NUMBER, -- The numeric value of the most recent Urine ACR test
    LATEST_ACR_DATE DATE, -- The date of the most recent Urine ACR test
    LATEST_EGFR_STAGE VARCHAR, -- CKD G-Stage (G1-G5) based solely on the latest eGFR value
    LATEST_ACR_STAGE VARCHAR, -- CKD A-Stage (A1-A3) based solely on the latest ACR value
    LATEST_CKD_STAGE_INFERRED VARCHAR, -- Combined CKD stage (e.g., 'G3a A2') inferred from the latest eGFR and ACR values
    LATEST_LABS_MEET_CKD_CRITERIA BOOLEAN, -- Flag: TRUE if the *latest* combination of eGFR/ACR indicates CKD presence (e.g., eGFR<60 or eGFR>=60&ACR>=3)
    -- Confirmation Flags (based on persistence over >90 days within a ~2 year window)
    HAS_CONFIRMED_LOW_EGFR BOOLEAN, -- Flag: TRUE if there are at least two eGFR < 60 results >= 90 days and <= 730 days apart
    HAS_CONFIRMED_HIGH_ACR BOOLEAN, -- Flag: TRUE if there are at least two ACR >= 3 results >= 90 days and <= 730 days apart
    HAS_CONFIRMED_CKD_BY_LABS BOOLEAN -- Flag: TRUE if either HAS_CONFIRMED_LOW_EGFR or HAS_CONFIRMED_HIGH_ACR is TRUE, indicating lab-confirmed CKD
)
COMMENT = 'Intermediate table inferring Chronic Kidney Disease (CKD) status and stage based on lab results (eGFR and ACR). It determines the latest G and A stages, calculates a combined inferred stage, and checks for confirmation of CKD based on persistent abnormal results (eGFR < 60 or ACR >= 3) over a >90 day period within approximately 2 years. Note: CKD Confirmation through labs does not mean the person should be clinically diagnosed with CKD, only that the lab results are persistent and consistent with a CKD diagnosis; they are strong candidates for case finding if not already diagnosed or if not coded with the correct stage of CKD. Use FCT_PERSON_DX_CKD to determine if the person has a clinical diagnosis of CKD.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS -- Use appropriate warehouse for your environment
AS

WITH AllEGFRWithStage AS (
    -- Calculates the G-Stage (G1-G5) for every historical eGFR result.
    -- Also flags if the individual result is < 60 (IS_LOW_EGFR).
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
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_EGFR_ALL -- Source: All historical eGFR results
),
AllACRWithStage AS (
    -- Calculates the A-Stage (A1-A3) for every historical Urine ACR result.
    -- Also flags if the individual result is >= 3 (IS_HIGH_ACR).
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
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_ALL -- Source: All historical ACR results
),
EGFRConfirmationCheck AS (
    -- For each eGFR result that is low (< 60), finds the date of the *immediately preceding* low eGFR result for the same person.
    -- Uses LAG function partitioned by person and ordered by date.
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE,
        IS_LOW_EGFR, -- Will always be TRUE due to WHERE clause below
        -- Look back for the date of the previous low eGFR result for the same person
        LAG(CASE WHEN IS_LOW_EGFR THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END, 1) IGNORE NULLS
            OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE ASC) AS PREV_LOW_EGFR_DATE
    FROM AllEGFRWithStage
    WHERE IS_LOW_EGFR -- Focus only on results that are already low
),
ACRConfirmationCheck AS (
    -- For each ACR result that is high (>= 3), finds the date of the *immediately preceding* high ACR result for the same person.
    -- Uses LAG function partitioned by person and ordered by date.
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE,
        IS_HIGH_ACR, -- Will always be TRUE due to WHERE clause below
        -- Look back for the date of the previous high ACR result for the same person
        LAG(CASE WHEN IS_HIGH_ACR THEN CLINICAL_EFFECTIVE_DATE ELSE NULL END, 1) IGNORE NULLS
            OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE ASC) AS PREV_HIGH_ACR_DATE
    FROM AllACRWithStage
    WHERE IS_HIGH_ACR -- Focus only on results that are already high
),
PersonLevelConfirmation AS (
    -- Determines, for each person, if they *ever* had two low eGFR results meeting the confirmation criteria.
    -- Checks if the time difference between any low eGFR and its preceding low eGFR is >= 90 days and <= 730 days (~2 years).
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
    -- Determines, for each person, if they *ever* had two high ACR results meeting the confirmation criteria.
    -- Checks if the time difference between any high ACR and its preceding high ACR is >= 90 days and <= 730 days (~2 years).
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
    -- Fetches the single most recent eGFR and ACR result for each person using the respective _LATEST tables.
    -- Uses a FULL OUTER JOIN to include persons who may only have one type of result.
    -- Recalculates the G and A stage based *only* on these latest results.
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
-- Final assembly: Combines the latest lab results/stages with the overall confirmation status for each person.
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
    -- Determines if the *latest* lab results meet the criteria for CKD diagnosis (ignoring confirmation period).
    -- CKD is indicated if eGFR < 60 OR if eGFR >= 60 AND ACR >= 3.
    CASE
        WHEN ll.LATEST_EGFR_STAGE IN ('G3a', 'G3b', 'G4', 'G5') THEN TRUE -- eGFR < 60
        WHEN ll.LATEST_EGFR_STAGE IN ('G1', 'G2') AND ll.LATEST_ACR_STAGE IN ('A2', 'A3') THEN TRUE -- eGFR >= 60 and ACR >= 3
        ELSE FALSE -- Otherwise, latest labs do not indicate CKD
    END AS LATEST_LABS_MEET_CKD_CRITERIA,
    -- Joins the eGFR confirmation result. Defaults to FALSE if no confirmation record exists for the person.
    COALESCE(plc.HAS_CONFIRMED_LOW_EGFR, FALSE) AS HAS_CONFIRMED_LOW_EGFR,
    -- Joins the ACR confirmation result. Defaults to FALSE if no confirmation record exists for the person.
    COALESCE(plac.HAS_CONFIRMED_HIGH_ACR, FALSE) AS HAS_CONFIRMED_HIGH_ACR,
    -- Determines overall lab confirmation status: TRUE if either eGFR or ACR confirmation criteria were met.
    (COALESCE(plc.HAS_CONFIRMED_LOW_EGFR, FALSE) OR COALESCE(plac.HAS_CONFIRMED_HIGH_ACR, FALSE)) AS HAS_CONFIRMED_CKD_BY_LABS
FROM LatestLabs ll
-- Left join confirmation results (per person) back to the latest lab results.
LEFT JOIN PersonLevelConfirmation plc ON ll.PERSON_ID = plc.PERSON_ID
LEFT JOIN PersonLevelACRConfirmation plac ON ll.PERSON_ID = plac.PERSON_ID;
