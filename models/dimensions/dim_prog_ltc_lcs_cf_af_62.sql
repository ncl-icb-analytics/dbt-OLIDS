CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_AF_62 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    HAS_PULSE_CHECK BOOLEAN, -- Flag indicating if person has had a pulse check in last 36 months
    LATEST_PULSE_CHECK_DATE DATE, -- Latest date of pulse check
    LATEST_HEALTH_CHECK_DATE DATE, -- Latest health check date
    ALL_PULSE_CHECK_CODES ARRAY, -- Array of all pulse check codes
    ALL_PULSE_CHECK_DISPLAYS ARRAY -- Array of all pulse check display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator AF_62: Patients over 65 missing pulse check in last 36 months. Only includes patients who meet all criteria.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population (already excludes those on AF register)
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        p.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE p
        ON bp.PERSON_ID = p.PERSON_ID
    WHERE p.AGE >= 65
),
PulseChecks AS (
    -- Get all pulse checks in last 36 months
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY,
        CLUSTER_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID = 'LCS_PULSE_RATE'
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -36, CURRENT_DATE())
),
PulseCheckSummary AS (
    -- Summarise pulse check status per person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_PULSE_CHECK_DATE,
        TRUE AS HAS_PULSE_CHECK,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_PULSE_CHECK_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_PULSE_CHECK_DISPLAYS
    FROM PulseChecks
    GROUP BY PERSON_ID, SK_PATIENT_ID
)
-- Final selection combining all criteria
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    COALESCE(pcs.HAS_PULSE_CHECK, FALSE) AS HAS_PULSE_CHECK,
    pcs.LATEST_PULSE_CHECK_DATE,
    hc.LATEST_HEALTH_CHECK_DATE,
    pcs.ALL_PULSE_CHECK_CODES,
    pcs.ALL_PULSE_CHECK_DISPLAYS
FROM BasePopulation bp
LEFT JOIN PulseCheckSummary pcs
    ON bp.PERSON_ID = pcs.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_HEALTH_CHECKS hc
    ON bp.PERSON_ID = hc.PERSON_ID
WHERE NOT COALESCE(pcs.HAS_PULSE_CHECK, FALSE);  -- Only include those without pulse checks 