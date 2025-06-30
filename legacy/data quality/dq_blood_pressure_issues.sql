-- Creates a dynamic table identifying Blood Pressure (BP) readings with potential data quality issues.
-- Includes checks for out-of-range values, ambiguous coding (e.g., BP_COD without specific SBP/DBP),
-- orphaned readings (SBP without DBP or vice-versa for the same event), and missing dates.

CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DQ_BLOOD_PRESSURE_ISSUES (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the blood pressure event (can be NULL if missing in source)
    SYSTOLIC_VALUE_ORIGINAL NUMBER, -- Consolidated systolic BP value before applying plausible range filters (can be out of range)
    DIASTOLIC_VALUE_ORIGINAL NUMBER, -- Consolidated diastolic BP value before applying plausible range filters (can be out of range)
    RESULT_UNIT_DISPLAY VARCHAR, -- Display value for the result unit (e.g., 'mmHg'), assumed consistent for the event
    ALL_CONCEPT_CODES ARRAY, -- Array of all unique concept codes contributing to this raw event
    ALL_CONCEPT_DISPLAYS ARRAY, -- Array of all unique concept display terms contributing to this raw event
    ALL_SOURCE_CLUSTER_IDS ARRAY, -- Array of all unique source cluster IDs contributing to this raw event
    -- DQ Flags
    IS_SBP_OUT_OF_RANGE BOOLEAN, -- Flag: TRUE if the consolidated systolic value is outside the plausible range (e.g., <40 or >350)
    IS_DBP_OUT_OF_RANGE BOOLEAN, -- Flag: TRUE if the consolidated diastolic value is outside the plausible range (e.g., <20 or >200)
    IS_CODING_AMBIGUOUS BOOLEAN, -- Flag: TRUE if BP_COD was used without corresponding specific SYSBP_COD or DIABP_COD for the event
    IS_ORPHANED_READING BOOLEAN, -- Flag: TRUE if only one of SBP/DBP is present for the person/date combination
    IS_DATE_MISSING BOOLEAN -- Flag: TRUE if the CLINICAL_EFFECTIVE_DATE for the original observation(s) was NULL
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Identifies potential data quality issues in raw Blood Pressure readings before final consolidation. Checks include: out-of-range values (SBP <40/>350, DBP <20/>200), ambiguous coding (BP_COD without specific SBP/DBP), orphaned readings (SBP/DBP mismatch per event), and missing dates.'
AS
WITH BaseObservationsAndClustersRaw AS (
    -- Selects individual BP-related observations, similar to INTERMEDIATE_BLOOD_PRESSURE_ALL,
    -- BUT keeps NULL dates and does NOT apply initial strict plausible value filters (only basic value presence).
    -- This allows identification of date issues and captures out-of-range values.
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
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.PCD_REFSET_LATEST AS C ON CON."code" = TO_VARCHAR(C.SNOMED_CODE)
    LEFT JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS UNIT_CON ON O."result_value_unit_concept_id" = UNIT_CON."id"
    WHERE C.CLUSTER_ID IN ('BP_COD', 'SYSBP_COD', 'DIABP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD')
      AND O."result_value" IS NOT NULL -- Still requires a value to assess, even if out of range.
),
RowFlagsRaw AS (
    -- Processes individual raw observation rows.
    -- Determines if a row represents a Systolic or Diastolic reading based on cluster ID or display text.
    -- (Context flags like IS_HOME_BP_ROW are not needed for DQ checks here).
    SELECT
        *,
        (SOURCE_CLUSTER_ID = 'SYSBP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%systolic%')) AS IS_SYSTOLIC_ROW,
        (SOURCE_CLUSTER_ID = 'DIABP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%diastolic%')) AS IS_DIASTOLIC_ROW
    FROM BaseObservationsAndClustersRaw
),
AggregatedRawEvents AS (
    -- Aggregates observations per event (Person + Date), even if the date is NULL.
    -- Pivots the original Systolic/Diastolic values without applying range filters at this stage.
    -- Collects traceability arrays and flags if specific SYSBP/DIABP codes were present for ambiguity checks.
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
        -- Flag if a specific Systolic code was present in the source observations for this event.
        BOOLOR_AGG(SOURCE_CLUSTER_ID = 'SYSBP_COD') AS HAD_SYSBP_COD,
        -- Flag if a specific Diastolic code was present in the source observations for this event.
        BOOLOR_AGG(SOURCE_CLUSTER_ID = 'DIABP_COD') AS HAD_DIABP_COD
    FROM RowFlagsRaw
    GROUP BY PERSON_ID, CLINICAL_EFFECTIVE_DATE -- Grouping by a potentially NULL date is allowed and intended here.
    -- Ensures that the aggregated event had at least one value (SBP or DBP) to be considered.
    HAVING SYSTOLIC_VALUE_ORIGINAL IS NOT NULL OR DIASTOLIC_VALUE_ORIGINAL IS NOT NULL
)
-- Final SELECT: Calculates specific Data Quality flags based on the aggregated raw event data.
SELECT
    PERSON_ID,
    CLINICAL_EFFECTIVE_DATE,
    SYSTOLIC_VALUE_ORIGINAL,
    DIASTOLIC_VALUE_ORIGINAL,
    RESULT_UNIT_DISPLAY,
    ALL_CONCEPT_CODES,
    ALL_CONCEPT_DISPLAYS,
    ALL_SOURCE_CLUSTER_IDS,
    -- DQ Flag: Check if the Systolic value exists and falls outside the defined plausible range (40-350).
    (CASE WHEN SYSTOLIC_VALUE_ORIGINAL IS NOT NULL AND (SYSTOLIC_VALUE_ORIGINAL < 40 OR SYSTOLIC_VALUE_ORIGINAL > 350) THEN TRUE ELSE FALSE END) AS IS_SBP_OUT_OF_RANGE,
    -- DQ Flag: Check if the Diastolic value exists and falls outside the defined plausible range (20-200).
    (CASE WHEN DIASTOLIC_VALUE_ORIGINAL IS NOT NULL AND (DIASTOLIC_VALUE_ORIGINAL < 20 OR DIASTOLIC_VALUE_ORIGINAL > 200) THEN TRUE ELSE FALSE END) AS IS_DBP_OUT_OF_RANGE,
    -- DQ Flag: Checks for coding ambiguity. TRUE if a generic BP_COD was used, but no specific SYSBP_COD or DIABP_COD was present for the same event.
    (CASE WHEN ARRAY_CONTAINS('BP_COD'::variant, ALL_SOURCE_CLUSTER_IDS) AND NOT HAD_SYSBP_COD AND NOT HAD_DIABP_COD THEN TRUE ELSE FALSE END) AS IS_CODING_AMBIGUOUS,
    -- DQ Flag: Checks for orphaned readings. TRUE if either SBP exists without DBP, or DBP exists without SBP for the event.
    (CASE WHEN (SYSTOLIC_VALUE_ORIGINAL IS NOT NULL AND DIASTOLIC_VALUE_ORIGINAL IS NULL) OR (SYSTOLIC_VALUE_ORIGINAL IS NULL AND DIASTOLIC_VALUE_ORIGINAL IS NOT NULL) THEN TRUE ELSE FALSE END) AS IS_ORPHANED_READING,
    -- DQ Flag: Checks if the CLINICAL_EFFECTIVE_DATE was NULL for this event during aggregation.
    (CLINICAL_EFFECTIVE_DATE IS NULL) AS IS_DATE_MISSING
FROM AggregatedRawEvents
-- Final Filter: Only includes rows where at least one of the data quality flags is TRUE.
WHERE
    IS_SBP_OUT_OF_RANGE = TRUE
 OR IS_DBP_OUT_OF_RANGE = TRUE
 OR IS_CODING_AMBIGUOUS = TRUE
 OR IS_ORPHANED_READING = TRUE
 OR IS_DATE_MISSING = TRUE;
