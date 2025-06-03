-- Template for LTC LCS Case Finding Indicators
-- Replace the following placeholders:
-- {INDICATOR_NAME} - e.g., AF_61, HF_62, etc.
-- {DESCRIPTION} - Brief description of what the indicator identifies
-- {CONDITION_CODES} - Conditions to exclude from base population (e.g., 'AF', 'HF')
-- {CLUSTER_IDS} - Cluster IDs for medications/observations to include (e.g., 'ORANTICOAG_2.8.2', 'DIGOXIN')
-- {MONTHS_ACTIVE} - Number of months for "active" medication filter (e.g., 3)
-- {HEALTH_CHECK_MONTHS} - Number of months for health check exclusion (e.g., 24)
-- {EXCLUSION_CLUSTER_IDS} - Cluster IDs for exclusion conditions (e.g., 'DVT', 'AF_FLUTTER')
-- {EXCLUSION_CONCEPT_CODES} - Specific SNOMED codes for exclusions (e.g., '1119304009', '62067003')

CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_LTC_LCS_CF_{INDICATOR_NAME} (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    IS_POTENTIAL_{INDICATOR_NAME} BOOLEAN, -- Flag indicating if person meets criteria
    HAS_ACTIVE_MEDICATION BOOLEAN, -- Flag indicating if person has active relevant medication
    LATEST_MEDICATION_DATE DATE, -- Latest date of relevant medication
    LATEST_HEALTH_CHECK_DATE DATE, -- Latest health check date
    HAS_EXCLUSION_CONDITION BOOLEAN, -- Flag indicating if person has any exclusion conditions
    EXCLUSION_REASON VARCHAR, -- Reason for exclusion if applicable
    ALL_MEDICATION_CODES ARRAY, -- Array of all relevant medication codes
    ALL_MEDICATION_DISPLAYS ARRAY -- Array of all relevant medication display terms
)
COMMENT = 'Dimension table for LTC LCS case finding indicator {INDICATOR_NAME}: {DESCRIPTION}'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePopulation AS (
    -- Use reusable base population and filter for specific condition exclusions
    SELECT DISTINCT
        PERSON_ID,
        SK_PATIENT_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_BASE_POPULATION
    WHERE CONDITION_CODE NOT IN ({CONDITION_CODES})
        OR (CONDITION_CODE IN ({CONDITION_CODES}) AND IS_ON_REGISTER = FALSE)
),
RelevantMedications AS (
    -- Get relevant medications for this indicator
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        CLINICAL_EFFECTIVE_DATE,
        CONCEPT_CODE,
        CONCEPT_DISPLAY,
        CLUSTER_ID
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE SOURCE_TABLE = 'MEDICATION_ORDER'
        AND CLUSTER_ID IN ({CLUSTER_IDS})
        AND CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -{MONTHS_ACTIVE}, CURRENT_DATE())
),
{INDICATOR_NAME}ExclusionConditions AS (
    -- Get patients with {INDICATOR_NAME} specific exclusion conditions
    SELECT
        PERSON_ID,
        TRUE AS HAS_EXCLUSION_CONDITION,
        LISTAGG(DISTINCT 
            CASE 
                WHEN CLUSTER_ID IN ({EXCLUSION_CLUSTER_IDS}) THEN CLUSTER_ID
                WHEN CONCEPT_CODE IN ({EXCLUSION_CONCEPT_CODES}) THEN CONCEPT_CODE
            END, ', ') AS EXCLUSION_REASON
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_RAW_DATA
    WHERE CLUSTER_ID IN ({EXCLUSION_CLUSTER_IDS})
        OR CONCEPT_CODE IN ({EXCLUSION_CONCEPT_CODES})
    GROUP BY PERSON_ID
),
MedicationSummary AS (
    -- Summarise medication status per person
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        MAX(CLINICAL_EFFECTIVE_DATE) AS LATEST_MEDICATION_DATE,
        TRUE AS HAS_ACTIVE_MEDICATION,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_MEDICATION_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_MEDICATION_DISPLAYS
    FROM RelevantMedications
    GROUP BY PERSON_ID, SK_PATIENT_ID
)
-- Final selection combining all criteria using reusable views where appropriate
SELECT
    bp.PERSON_ID,
    bp.SK_PATIENT_ID,
    -- Person meets criteria if: has medication, no recent health check, no exclusions
    CASE 
        WHEN ms.HAS_ACTIVE_MEDICATION
            AND NOT COALESCE(hc.HAS_RECENT_HEALTH_CHECK_{HEALTH_CHECK_MONTHS}M, FALSE)
            AND (ec.HAS_EXCLUSION_CONDITION IS NULL OR ec.HAS_EXCLUSION_CONDITION = FALSE)
        THEN TRUE
        ELSE FALSE
    END AS IS_POTENTIAL_{INDICATOR_NAME},
    COALESCE(ms.HAS_ACTIVE_MEDICATION, FALSE) AS HAS_ACTIVE_MEDICATION,
    ms.LATEST_MEDICATION_DATE,
    hc.LATEST_HEALTH_CHECK_DATE,
    COALESCE(ec.HAS_EXCLUSION_CONDITION, FALSE) AS HAS_EXCLUSION_CONDITION,
    ec.EXCLUSION_REASON,
    ms.ALL_MEDICATION_CODES,
    ms.ALL_MEDICATION_DISPLAYS
FROM BasePopulation bp
LEFT JOIN MedicationSummary ms
    ON bp.PERSON_ID = ms.PERSON_ID
LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LTC_LCS_CF_HEALTH_CHECKS hc
    ON bp.PERSON_ID = hc.PERSON_ID
LEFT JOIN {INDICATOR_NAME}ExclusionConditions ec
    ON bp.PERSON_ID = ec.PERSON_ID; 