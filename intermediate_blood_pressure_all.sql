-- Creates a dynamic table consolidating all valid Blood Pressure (BP) readings (Systolic/Diastolic)
-- from the Observation table into single events per person per date.
-- Filters out readings with NULL dates or implausible values.
-- Determines context flags (Home/ABPM) based on associated codes.
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the blood pressure reading event (ensured to be NOT NULL)
    SYSTOLIC_VALUE NUMBER, -- Consolidated systolic blood pressure value for the event date
    DIASTOLIC_VALUE NUMBER, -- Consolidated diastolic blood pressure value for the event date
    IS_HOME_BP_EVENT BOOLEAN, -- Flag: TRUE if any observation on this date for this person was coded as a Home BP reading
    IS_ABPM_BP_EVENT BOOLEAN, -- Flag: TRUE if any observation on this date for this person was coded as an ABPM reading
    RESULT_UNIT_DISPLAY VARCHAR, -- Display value for the result unit (e.g., 'mmHg'), assumed consistent for the event
    SYSTOLIC_OBSERVATION_ID VARCHAR, -- Observation ID associated with the systolic reading for this event (if identifiable)
    DIASTOLIC_OBSERVATION_ID VARCHAR, -- Observation ID associated with the diastolic reading for this event (if identifiable)
    ALL_OBSERVATION_IDS ARRAY, -- Array of all unique observation IDs contributing to this person-date event
    ALL_CONCEPT_CODES ARRAY, -- Array of all unique concept codes contributing to this event
    ALL_CONCEPT_DISPLAYS ARRAY, -- Array of all unique concept display terms contributing to this event
    ALL_SOURCE_CLUSTER_IDS ARRAY -- Array of all unique source cluster IDs contributing to this event
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Intermediate table consolidating valid, dated Blood Pressure readings (Clinic, Home, ABPM) into one row per person-date event. Filters out NULL dates and implausible values (e.g., SBP < 40 or > 350). Includes traceability arrays and aggregated context flags (Home/ABPM).'
AS
WITH BaseObservationsAndClusters AS (
    -- Selects individual BP-related observations (Systolic, Diastolic, BP code, Home, ABPM).
    -- Joins to terminology tables (CONCEPT_MAP, CONCEPT, PCD_REFSET_LATEST) to get codes, descriptions, and cluster IDs.
    -- Applies initial filters: must have a result value, must have a non-NULL date, and result value must be within a very broad plausible range (20-350).
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
      AND O."result_value" < 350 -- High threshold applied broadly here (refined later)
),
RowFlags AS (
    -- Processes individual observation rows from BaseObservationsAndClusters.
    -- Determines if a row represents a Systolic or Diastolic reading based on cluster ID or display text.
    -- Determines if a row indicates a Home BP or ABPM context based on its cluster ID.
    SELECT
        *,
        -- Flag for Systolic rows: checks for specific cluster ID or if display text contains 'systolic'.
        (SOURCE_CLUSTER_ID = 'SYSBP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%systolic%')) AS IS_SYSTOLIC_ROW,
        -- Flag for Diastolic rows: checks for specific cluster ID or if display text contains 'diastolic'.
        (SOURCE_CLUSTER_ID = 'DIABP_COD' OR (SOURCE_CLUSTER_ID = 'BP_COD' AND CONCEPT_DISPLAY ILIKE '%diastolic%')) AS IS_DIASTOLIC_ROW,
        -- Flag for Home BP context rows.
        (SOURCE_CLUSTER_ID IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS IS_HOME_BP_ROW,
        -- Flag for ABPM context rows.
        (SOURCE_CLUSTER_ID = 'ABPM_COD') AS IS_ABPM_BP_ROW
    FROM BaseObservationsAndClusters
)
-- Final Aggregation: Groups observations by Person and Date to create a single BP event per day.
SELECT DISTINCT -- Using DISTINCT primarily because the aggregation itself should produce unique Person-Date rows.
    PERSON_ID,
    CLINICAL_EFFECTIVE_DATE,
    -- Pivots Systolic/Diastolic values: Takes the MAX value identified as Systolic/Diastolic for the person-date group.
    MAX(CASE WHEN IS_SYSTOLIC_ROW THEN RESULT_VALUE ELSE NULL END) AS SYSTOLIC_VALUE,
    MAX(CASE WHEN IS_DIASTOLIC_ROW THEN RESULT_VALUE ELSE NULL END) AS DIASTOLIC_VALUE,
    -- Aggregates context flags: If any row for the event was Home/ABPM, the event flag is TRUE.
    BOOLOR_AGG(IS_HOME_BP_ROW) AS IS_HOME_BP_EVENT,
    BOOLOR_AGG(IS_ABPM_BP_ROW) AS IS_ABPM_BP_EVENT,
    -- Aggregates traceability fields: Takes one unit display, aggregates unique codes/displays/clusters, identifies specific SBP/DBP observation IDs if possible.
    ANY_VALUE(RESULT_UNIT_DISPLAY) AS RESULT_UNIT_DISPLAY,
    MAX(CASE WHEN IS_SYSTOLIC_ROW THEN OBSERVATION_ID ELSE NULL END) AS SYSTOLIC_OBSERVATION_ID,
    MAX(CASE WHEN IS_DIASTOLIC_ROW THEN OBSERVATION_ID ELSE NULL END) AS DIASTOLIC_OBSERVATION_ID,
    ARRAY_AGG(DISTINCT OBSERVATION_ID) WITHIN GROUP (ORDER BY OBSERVATION_ID) AS ALL_OBSERVATION_IDS,
    ARRAY_AGG(DISTINCT CONCEPT_CODE) WITHIN GROUP (ORDER BY CONCEPT_CODE) AS ALL_CONCEPT_CODES,
    ARRAY_AGG(DISTINCT CONCEPT_DISPLAY) WITHIN GROUP (ORDER BY CONCEPT_DISPLAY) AS ALL_CONCEPT_DISPLAYS,
    ARRAY_AGG(DISTINCT SOURCE_CLUSTER_ID) WITHIN GROUP (ORDER BY SOURCE_CLUSTER_ID) AS ALL_SOURCE_CLUSTER_IDS
FROM RowFlags
GROUP BY PERSON_ID, CLINICAL_EFFECTIVE_DATE
-- HAVING clause filters the results after grouping:
-- 1. Ensures that each consolidated event has at least one BP value (either Systolic or Diastolic).
-- 2. Applies more specific plausible range checks to the consolidated Systolic (>=40, <=350) and Diastolic (>=20, <=200) values, allowing NULLs if the other value exists.
HAVING (SYSTOLIC_VALUE IS NOT NULL OR DIASTOLIC_VALUE IS NOT NULL)
   AND (SYSTOLIC_VALUE IS NULL OR (SYSTOLIC_VALUE >= 40 AND SYSTOLIC_VALUE <= 350))
   AND (DIASTOLIC_VALUE IS NULL OR (DIASTOLIC_VALUE >= 20 AND DIASTOLIC_VALUE <= 200));



