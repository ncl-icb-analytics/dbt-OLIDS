CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_NDH (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_NDH_REGISTER BOOLEAN, -- Flag indicating if person is on the NDH register
    HAS_NDH_DIAGNOSIS BOOLEAN, -- Flag indicating if person has an NDH diagnosis
    HAS_IGT_DIAGNOSIS BOOLEAN, -- Flag indicating if person has an IGT diagnosis
    HAS_PRD_DIAGNOSIS BOOLEAN, -- Flag indicating if person has a PRD diagnosis
    HAS_DIABETES_DIAGNOSIS BOOLEAN, -- Flag indicating if person has ever had diabetes
    IS_DIABETES_RESOLVED BOOLEAN, -- Flag indicating if person's diabetes is resolved
    EARLIEST_NDH_DATE DATE, -- Earliest NDH diagnosis date
    EARLIEST_IGT_DATE DATE, -- Earliest IGT diagnosis date
    EARLIEST_PRD_DATE DATE, -- Earliest PRD diagnosis date
    EARLIEST_MULTNDH_DATE DATE, -- Earliest of NDH, IGT, or PRD diagnosis dates
    LATEST_NDH_DATE DATE, -- Latest NDH diagnosis date
    LATEST_IGT_DATE DATE, -- Latest IGT diagnosis date
    LATEST_PRD_DATE DATE, -- Latest PRD diagnosis date
    LATEST_MULTNDH_DATE DATE, -- Latest of NDH, IGT, or PRD diagnosis dates
    EARLIEST_DIABETES_DATE DATE, -- Earliest diabetes diagnosis date
    LATEST_DIABETES_DATE DATE, -- Latest diabetes diagnosis date
    LATEST_DIABETES_RESOLUTION_DATE DATE, -- Latest diabetes resolution date
    ALL_NDH_CONCEPT_CODES ARRAY, -- All NDH concept codes for this person
    ALL_NDH_CONCEPT_DISPLAYS ARRAY, -- All NDH concept display terms for this person
    ALL_IGT_CONCEPT_CODES ARRAY, -- All IGT concept codes for this person
    ALL_IGT_CONCEPT_DISPLAYS ARRAY, -- All IGT concept display terms for this person
    ALL_PRD_CONCEPT_CODES ARRAY, -- All PRD concept codes for this person
    ALL_PRD_CONCEPT_DISPLAYS ARRAY, -- All PRD concept display terms for this person
    ALL_DIABETES_CONCEPT_CODES ARRAY, -- All diabetes concept codes for this person
    ALL_DIABETES_CONCEPT_DISPLAYS ARRAY -- All diabetes concept display terms for this person
)
COMMENT = 'Fact table for NDH register. Implements core business rules for non-diabetic hyperglycaemia register inclusion.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH FilteredByAge AS (
    -- Get all relevant patients with their age
    SELECT 
        COALESCE(ndh.PERSON_ID, dm.PERSON_ID) AS PERSON_ID,
        COALESCE(ndh.SK_PATIENT_ID, dm.SK_PATIENT_ID) AS SK_PATIENT_ID,
        age.AGE,
        -- NDH-related fields
        ndh.EARLIEST_NDH_DATE,
        ndh.EARLIEST_IGT_DATE,
        ndh.EARLIEST_PRD_DATE,
        ndh.EARLIEST_MULTNDH_DATE,
        ndh.LATEST_NDH_DATE,
        ndh.LATEST_IGT_DATE,
        ndh.LATEST_PRD_DATE,
        ndh.LATEST_MULTNDH_DATE,
        ndh.ALL_NDH_CONCEPT_CODES,
        ndh.ALL_NDH_CONCEPT_DISPLAYS,
        ndh.ALL_IGT_CONCEPT_CODES,
        ndh.ALL_IGT_CONCEPT_DISPLAYS,
        ndh.ALL_PRD_CONCEPT_CODES,
        ndh.ALL_PRD_CONCEPT_DISPLAYS,
        -- Diabetes-related fields
        dm.EARLIEST_DIABETES_DATE,
        dm.LATEST_DIABETES_DATE,
        dm.LATEST_RESOLUTION_DATE AS LATEST_DIABETES_RESOLUTION_DATE,
        dm.IS_DIABETES_RESOLVED_FLAG AS IS_DIABETES_RESOLVED,
        dm.ALL_DIABETES_CONCEPT_CODES,
        dm.ALL_DIABETES_CONCEPT_DISPLAYS
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_NDH_DIAGNOSES ndh
    FULL OUTER JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_DIABETES_DIAGNOSES dm
        ON ndh.PERSON_ID = dm.PERSON_ID
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        ON COALESCE(ndh.PERSON_ID, dm.PERSON_ID) = age.PERSON_ID
    WHERE age.AGE >= 18 -- Rule 1: Reject patients under 18
)
-- Final selection implementing business rules
SELECT
    f.PERSON_ID,
    f.SK_PATIENT_ID,
    f.AGE,
    -- Implement business rules for register inclusion
    CASE
        -- Rule 2: Has NDH diagnosis and never had diabetes
        WHEN f.EARLIEST_MULTNDH_DATE IS NOT NULL 
            AND f.EARLIEST_DIABETES_DATE IS NULL THEN TRUE
        -- Rule 3: Has NDH diagnosis and latest diabetes is resolved
        WHEN f.EARLIEST_MULTNDH_DATE IS NOT NULL 
            AND f.LATEST_DIABETES_DATE IS NOT NULL 
            AND f.LATEST_DIABETES_RESOLUTION_DATE IS NOT NULL 
            AND f.IS_DIABETES_RESOLVED THEN TRUE
        ELSE FALSE
    END AS IS_ON_NDH_REGISTER,
    f.EARLIEST_NDH_DATE IS NOT NULL AS HAS_NDH_DIAGNOSIS,
    f.EARLIEST_IGT_DATE IS NOT NULL AS HAS_IGT_DIAGNOSIS,
    f.EARLIEST_PRD_DATE IS NOT NULL AS HAS_PRD_DIAGNOSIS,
    f.EARLIEST_DIABETES_DATE IS NOT NULL AS HAS_DIABETES_DIAGNOSIS,
    COALESCE(f.IS_DIABETES_RESOLVED, FALSE) AS IS_DIABETES_RESOLVED,
    f.EARLIEST_NDH_DATE,
    f.EARLIEST_IGT_DATE,
    f.EARLIEST_PRD_DATE,
    f.EARLIEST_MULTNDH_DATE,
    f.LATEST_NDH_DATE,
    f.LATEST_IGT_DATE,
    f.LATEST_PRD_DATE,
    f.LATEST_MULTNDH_DATE,
    f.EARLIEST_DIABETES_DATE,
    f.LATEST_DIABETES_DATE,
    f.LATEST_DIABETES_RESOLUTION_DATE,
    f.ALL_NDH_CONCEPT_CODES,
    f.ALL_NDH_CONCEPT_DISPLAYS,
    f.ALL_IGT_CONCEPT_CODES,
    f.ALL_IGT_CONCEPT_DISPLAYS,
    f.ALL_PRD_CONCEPT_CODES,
    f.ALL_PRD_CONCEPT_DISPLAYS,
    f.ALL_DIABETES_CONCEPT_CODES,
    f.ALL_DIABETES_CONCEPT_DISPLAYS
FROM FilteredByAge f; 