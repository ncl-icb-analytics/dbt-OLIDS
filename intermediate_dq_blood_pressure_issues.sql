
-- Creates a dynamic table identifying Blood Pressure (BP) readings with potential data quality issues.
-- Includes checks for out-of-range values, ambiguous coding (e.g., BP_COD without specific SBP/DBP),
-- orphaned readings (SBP without DBP or vice-versa for the same event), and missing dates.

CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_DQ_BLOOD_PRESSURE_ISSUES (
    PERSON_ID VARCHAR,
    CLINICAL_EFFECTIVE_DATE DATE,         -- Can be NULL for the IS_DATE_MISSING flag
    SYSTOLIC_VALUE_ORIGINAL NUMBER,       -- Original pivoted systolic value (can be out of range)
    DIASTOLIC_VALUE_ORIGINAL NUMBER,      -- Original pivoted diastolic value (can be out of range)
    RESULT_UNIT_DISPLAY VARCHAR,
    ALL_OBSERVATION_IDS ARRAY,
    ALL_CONCEPT_CODES ARRAY,
    ALL_CONCEPT_DISPLAYS ARRAY,
    ALL_SOURCE_CLUSTER_IDS ARRAY,
    -- DQ Flags
    IS_SBP_OUT_OF_RANGE BOOLEAN,          -- Systolic value is outside the plausible range (e.g., <40 or >350)
    IS_DBP_OUT_OF_RANGE BOOLEAN,          -- Diastolic value is outside the plausible range (e.g., <20 or >200)
    IS_CODING_AMBIGUOUS BOOLEAN,          -- Ambiguous coding (e.g., BP_COD used without specific SYSBP/DIABP codes)
    IS_ORPHANED_READING BOOLEAN,          -- Only one of SBP/DBP is present for the person/date combination
    IS_DATE_MISSING BOOLEAN               -- Added: The CLINICAL_EFFECTIVE_DATE for the observation was NULL
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Identifies BP readings with potential DQ issues: out-of-range values (SBP <40/>350, DBP <20/>200), ambiguous coding, orphaned readings (SBP/DBP mismatch per event), or missing dates.'
AS
WITH BaseObservationsAndClustersRaw AS (
    -- Select relevant BP observations, join to concepts/codesets, WITHOUT date or value filters initially
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE, -- Keep NULL dates here
        O."result_value" AS RESULT_VALUE,
        UNIT_CON."display" AS RESULT_UNIT_DISPLAY,
        CON."id" AS CONCEPT_ID,
        CON."system" AS CONCEPT_SYSTEM,
        CON."code" AS CONCEPT_CODE,
        C.SNOMED_CODE_DESCRIPTION AS CONCEPT_DISPLAY,
        C.CLUSTER_ID AS SOURCE_CLUSTER_ID
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP ON O."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP AS MAP ON O."observation_core_concept_id" = MAP."source_code_id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS CON ON MAP."target_code_id" = CON."id"
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.PCD_REFSET_LATEST AS C ON CON."code" = TO_VARCHAR(C.SNOMED_CODE)
    LEFT JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS UNIT_CON ON O."result_value_unit_concept_id" = UNIT_CON."id"
    WHERE C.CLUSTER_ID IN ('BP_COD', 'SYSBP_COD', 'DIABP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD')
      AND O."result_value" IS NOT NULL -- Still require a value to assess
),
RowFlagsRaw AS (
    -- Determine flags for each individual observation row before aggregation
    SELECT
        *,
        (SOURCE_CLUSTER_ID = 'SYSBP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%systolic%')) AS IS_SYSTOLIC_ROW,
        (SOURCE_CLUSTER_ID = 'DIABP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%diastolic%')) AS IS_DIASTOLIC_ROW
    FROM BaseObservationsAndClustersRaw
),
AggregatedRawEvents AS (
    -- Aggregate per event (Person + Time), handling potential NULL dates in GROUP BY
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE, -- This can be NULL
        MAX(CASE WHEN IS_SYSTOLIC_ROW THEN RESULT_VALUE ELSE NULL END) AS SYSTOLIC_VALUE_ORIGINAL,
        MAX(CASE WHEN IS_DIASTOLIC_ROW THEN RESULT_VALUE ELSE NULL END) AS DIASTOLIC_VALUE_ORIGINAL,
        ANY_VALUE(RESULT_UNIT_DISPLAY) AS RESULT_UNIT_DISPLAY,
        ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_OBSERVATION_IDS,
        ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_CONCEPT_CODES,
        ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_CONCEPT_DISPLAYS,
        ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_SOURCE_CLUSTER_IDS,
        -- Check if specific codes were originally present for ambiguity check
        BOOLOR_AGG(SOURCE_CLUSTER_ID = 'SYSBP_COD') AS HAD_SYSBP_COD,
        BOOLOR_AGG(SOURCE_CLUSTER_ID = 'DIABP_COD') AS HAD_DIABP_COD
    FROM RowFlagsRaw
    GROUP BY PERSON_ID, CLINICAL_EFFECTIVE_DATE -- Grouping by NULL date is allowed
    -- Ensure at least one value was present for the event
    HAVING SYSTOLIC_VALUE_ORIGINAL IS NOT NULL OR DIASTOLIC_VALUE_ORIGINAL IS NOT NULL
)
-- Calculate DQ flags based on the raw pivoted events
SELECT
    PERSON_ID,
    CLINICAL_EFFECTIVE_DATE,
    SYSTOLIC_VALUE_ORIGINAL,
    DIASTOLIC_VALUE_ORIGINAL,
    RESULT_UNIT_DISPLAY,
    ALL_OBSERVATION_IDS,
    ALL_CONCEPT_CODES,
    ALL_CONCEPT_DISPLAYS,
    ALL_SOURCE_CLUSTER_IDS,
    -- DQ Flags
    -- Check if SBP is present and outside plausible range (e.g., 40-350)
    (CASE WHEN SYSTOLIC_VALUE_ORIGINAL IS NOT NULL AND (SYSTOLIC_VALUE_ORIGINAL < 40 OR SYSTOLIC_VALUE_ORIGINAL > 350) THEN TRUE ELSE FALSE END) AS IS_SBP_OUT_OF_RANGE,
    -- Check if DBP is present and outside plausible range (e.g., 20-200)
    (CASE WHEN DIASTOLIC_VALUE_ORIGINAL IS NOT NULL AND (DIASTOLIC_VALUE_ORIGINAL < 20 OR DIASTOLIC_VALUE_ORIGINAL > 200) THEN TRUE ELSE FALSE END) AS IS_DBP_OUT_OF_RANGE,
    -- Ambiguous if BP_COD was used and specific SBP/DBP codes were NOT used
    (CASE WHEN ARRAY_CONTAINS('BP_COD'::variant, ALL_SOURCE_CLUSTER_IDS) AND NOT HAD_SYSBP_COD AND NOT HAD_DIABP_COD THEN TRUE ELSE FALSE END) AS IS_CODING_AMBIGUOUS,
    -- Orphan if one reading exists but the other is NULL for this event time
    (CASE WHEN (SYSTOLIC_VALUE_ORIGINAL IS NOT NULL AND DIASTOLIC_VALUE_ORIGINAL IS NULL) OR (SYSTOLIC_VALUE_ORIGINAL IS NULL AND DIASTOLIC_VALUE_ORIGINAL IS NOT NULL) THEN TRUE ELSE FALSE END) AS IS_ORPHANED_READING,
    -- *** Added DQ Flag: Check if the date was originally NULL ***
    (CLINICAL_EFFECTIVE_DATE IS NULL) AS IS_DATE_MISSING
FROM AggregatedRawEvents
-- Filter to include only rows with at least one DQ issue flagged
WHERE
    IS_SBP_OUT_OF_RANGE = TRUE
 OR IS_DBP_OUT_OF_RANGE = TRUE
 OR IS_CODING_AMBIGUOUS = TRUE
 OR IS_ORPHANED_READING = TRUE
 OR IS_DATE_MISSING = TRUE; -- Include rows where the date was missing