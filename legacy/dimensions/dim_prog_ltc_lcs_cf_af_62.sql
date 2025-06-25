CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_AF_62 (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the patient
    HAS_PULSE_CHECK BOOLEAN, -- Flag indicating if person has had a pulse check
    LATEST_PULSE_CHECK_DATE DATE, -- Latest date of pulse check
    LATEST_HEALTH_CHECK_DATE DATE, -- Latest health check date
    HAS_EXCLUSION_CONDITION BOOLEAN, -- Flag indicating if person has any exclusion conditions
    EXCLUSION_REASON VARCHAR, -- Reason for exclusion if applicable
    ALL_PULSE_CHECK_CODES ARRAY, -- Array of all pulse check codes
    ALL_PULSE_CHECK_DISPLAYS ARRAY -- Array of all pulse check display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator AF_62: Patients over 65 who are missing pulse checks. Only includes patients who meet all criteria.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Get base population of patients over 65
    -- Note: Base population already excludes those on the AF register
    SELECT DISTINCT
        bp.PERSON_ID,
        bp.SK_PATIENT_ID,
        age.AGE
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION bp
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        USING (PERSON_ID)
    WHERE age.AGE >= 65
),
PulseChecks AS (
    -- Get pulse check data from last 36 months
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE SOURCE_TABLE = 'OBSERVATION'
        AND CLUSTER_ID IN ('PULSE_RATE', 'PULSE_RHYTHM')
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -36, CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1
),
PulseCheckSummary AS (
    -- Summarise pulse check status per person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE AS LATEST_PULSE_CHECK_DATE,
        TRUE AS HAS_PULSE_CHECK,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_PULSE_CHECK_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_PULSE_CHECK_DISPLAYS
    FROM PulseChecks
    GROUP BY PERSON_ID, SK_PATIENT_ID, CLINICAL_EFFECTIVE_DATE
)
-- Only include patients who meet ALL criteria for AF_62
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    bp.AGE,
    COALESCE(pcs.HAS_PULSE_CHECK, FALSE) AS HAS_PULSE_CHECK,
    pcs.LATEST_PULSE_CHECK_DATE,
    hc.LATEST_HEALTH_CHECK_DATE,
    FALSE AS HAS_EXCLUSION_CONDITION, -- No exclusion conditions for AF_62
    NULL AS EXCLUSION_REASON,
    pcs.ALL_PULSE_CHECK_CODES,
    pcs.ALL_PULSE_CHECK_DISPLAYS
FROM BasePopulation bp
LEFT JOIN PulseCheckSummary pcs
    USING (PERSON_ID, SK_PATIENT_ID)
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_HEALTH_CHECKS hc
    USING (PERSON_ID)
WHERE NOT COALESCE(pcs.HAS_PULSE_CHECK, FALSE)
    AND NOT COALESCE(hc.HAS_RECENT_HEALTH_CHECK_24M, FALSE);
