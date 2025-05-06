-- Creates a dynamic table consolidating all valid Blood Pressure (BP) readings (Systolic/Diastolic)
-- from the Observation table into single events per person per date.
-- Filters out readings with NULL dates or implausible values.
-- Determines context flags (Home/ABPM) based on associated codes.
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_ALL (
    PERSON_ID VARCHAR,
    CLINICAL_EFFECTIVE_DATE DATE, -- Ensured to be NOT NULL by filtering
    SYSTOLIC_VALUE NUMBER,
    DIASTOLIC_VALUE NUMBER,
    IS_HOME_BP_EVENT BOOLEAN,
    IS_ABPM_BP_EVENT BOOLEAN,
    RESULT_UNIT_DISPLAY VARCHAR,
    SYSTOLIC_OBSERVATION_ID VARCHAR,
    DIASTOLIC_OBSERVATION_ID VARCHAR,
    ALL_OBSERVATION_IDS ARRAY,
    ALL_CONCEPT_CODES ARRAY,
    ALL_CONCEPT_DISPLAYS ARRAY,
    ALL_SOURCE_CLUSTER_IDS ARRAY
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Consolidates valid, dated BP readings (Clinic, Home, ABPM) into one row per person-date event. Filters out NULL dates and implausible values (SBP < 40 or > 350, DBP < 20 or > 200). Includes traceability and context flags.'
AS
WITH BaseObservationsAndClusters AS (
    -- Select relevant BP observations, join to concepts/codesets, and apply initial filters
    SELECT
        O."id" AS OBSERVATION_ID,
        PP."person_id" AS PERSON_ID,
        O."clinical_effective_date" AS CLINICAL_EFFECTIVE_DATE,
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
      AND O."result_value" IS NOT NULL
      -- **** Added Filter: Ensure the date is not NULL ****
      AND O."clinical_effective_date" IS NOT NULL
      -- Apply plausible value filter (can be adjusted)
      AND O."result_value" > 20 -- Low threshold for either SBP or DBP
      AND O."result_value" < 350 -- High threshold for SBP
),
RowFlags AS (
    -- Determine flags for each individual observation row before aggregation
    SELECT
        *,
        -- Determine if row represents Systolic or Diastolic based on cluster/display
        (SOURCE_CLUSTER_ID = 'SYSBP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%systolic%')) AS IS_SYSTOLIC_ROW,
        (SOURCE_CLUSTER_ID = 'DIABP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%diastolic%')) AS IS_DIASTOLIC_ROW,
        -- Determine context flags per row
        (SOURCE_CLUSTER_ID IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS IS_HOME_BP_ROW,
        (SOURCE_CLUSTER_ID = 'ABPM_COD') AS IS_ABPM_BP_ROW
    FROM BaseObservationsAndClusters
)
-- Final Aggregation per event (Person + Date)
SELECT DISTINCT
    PERSON_ID,
    CLINICAL_EFFECTIVE_DATE,
    -- Pivot values based on row flags
    MAX(CASE WHEN IS_SYSTOLIC_ROW THEN RESULT_VALUE ELSE NULL END) AS SYSTOLIC_VALUE,
    MAX(CASE WHEN IS_DIASTOLIC_ROW THEN RESULT_VALUE ELSE NULL END) AS DIASTOLIC_VALUE,
    -- Aggregate event flags
    BOOLOR_AGG(IS_HOME_BP_ROW) AS IS_HOME_BP_EVENT,
    BOOLOR_AGG(IS_ABPM_BP_ROW) AS IS_ABPM_BP_EVENT,
    -- Aggregate traceability/context fields
    ANY_VALUE(RESULT_UNIT_DISPLAY) AS RESULT_UNIT_DISPLAY,
    MAX(CASE WHEN IS_SYSTOLIC_ROW THEN OBSERVATION_ID ELSE NULL END) AS SYSTOLIC_OBSERVATION_ID,
    MAX(CASE WHEN IS_DIASTOLIC_ROW THEN OBSERVATION_ID ELSE NULL END) AS DIASTOLIC_OBSERVATION_ID,
    ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_OBSERVATION_IDS,
    ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_CONCEPT_CODES,
    ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_CONCEPT_DISPLAYS,
    ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_SOURCE_CLUSTER_IDS
FROM RowFlags
GROUP BY PERSON_ID, CLINICAL_EFFECTIVE_DATE
-- Ensure the pivoted row has at least one BP value (Systolic or Diastolic)
-- Also apply plausible range check here on the pivoted values
HAVING (SYSTOLIC_VALUE IS NOT NULL OR DIASTOLIC_VALUE IS NOT NULL)
   AND (SYSTOLIC_VALUE IS NULL OR (SYSTOLIC_VALUE >= 40 AND SYSTOLIC_VALUE <= 350)) -- Allow NULL SBP if DBP exists, but check range if SBP exists
   AND (DIASTOLIC_VALUE IS NULL OR (DIASTOLIC_VALUE >= 20 AND DIASTOLIC_VALUE <= 200)); -- Allow NULL DBP if SBP exists, but check range if DBP exists



