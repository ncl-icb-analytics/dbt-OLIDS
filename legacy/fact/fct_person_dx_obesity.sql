CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_OBESITY (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age in years
    IS_ON_OBESITY_REGISTER BOOLEAN, -- Flag indicating if person is on obesity register
    IS_BAME BOOLEAN, -- Flag indicating if person is BAME
    HAS_BMI_30_PLUS BOOLEAN, -- Flag indicating if person has BMI >= 30
    HAS_BMI_27_5_PLUS BOOLEAN, -- Flag indicating if person has BMI >= 27.5
    EARLIEST_BMI_DATE DATE, -- Earliest BMI recording date
    LATEST_BMI_DATE DATE, -- Latest BMI recording date
    LATEST_VALID_BMI_DATE DATE, -- Latest valid BMI recording date
    LATEST_VALID_BMI_VALUE NUMBER, -- Latest valid BMI value
    LATEST_ETHNICITY_DATE DATE, -- Latest ethnicity recording date
    LATEST_BAME_DATE DATE, -- Latest BAME ethnicity recording date
    ALL_BMI_CONCEPT_CODES ARRAY, -- All BMI concept codes for this person
    ALL_BMI_CONCEPT_DISPLAYS ARRAY, -- All BMI concept display terms for this person
    ALL_ETHNICITY_CONCEPT_CODES ARRAY, -- All ethnicity concept codes for this person
    ALL_ETHNICITY_CONCEPT_DISPLAYS ARRAY -- All ethnicity concept display terms for this person
)
COMMENT = 'Fact table for obesity register. Includes patients aged 18 and over who meet either: 1) BMI >= 30, or 2) BMI >= 27.5 for BAME patients. Tracks BMI values, dates, and ethnicity status.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH FilteredByAge AS (
    -- Get base population aged 18 and over
    SELECT
        COALESCE(b.PERSON_ID, e.PERSON_ID) AS PERSON_ID,
        COALESCE(b.SK_PATIENT_ID, e.SK_PATIENT_ID) AS SK_PATIENT_ID,
        a.AGE,
        b.IS_BMI_30_PLUS,
        b.IS_BMI_27_5_PLUS,
        b.LATEST_BMI_DATE,
        b.LATEST_VALID_BMI_DATE,
        b.LATEST_VALID_BMI_VALUE,
        b.ALL_BMI_CONCEPT_CODES,
        b.ALL_BMI_CONCEPT_DISPLAYS,
        e.IS_BAME,
        e.LATEST_ETHNICITY_DATE,
        e.LATEST_BAME_DATE,
        e.ALL_ETHNICITY_CONCEPT_CODES,
        e.ALL_ETHNICITY_CONCEPT_DISPLAYS
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BMI_VALUES b
    FULL OUTER JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_ETHNICITY_QOF e
        ON b.PERSON_ID = e.PERSON_ID
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE a
        ON COALESCE(b.PERSON_ID, e.PERSON_ID) = a.PERSON_ID
    WHERE a.AGE >= 18 -- Rule 1: Age filter
)
-- Final selection implementing business rules
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    AGE,
    -- Rule 2 & 3: Obesity register inclusion based on BMI and ethnicity
    CASE
        WHEN IS_BMI_30_PLUS THEN TRUE -- Rule 2: BMI >= 30
        WHEN IS_BAME AND IS_BMI_27_5_PLUS THEN TRUE -- Rule 3: BAME with BMI >= 27.5
        ELSE FALSE
    END AS IS_ON_OBESITY_REGISTER,
    IS_BAME,
    IS_BMI_30_PLUS,
    IS_BMI_27_5_PLUS,
    MIN(LATEST_BMI_DATE) OVER (PARTITION BY PERSON_ID) AS EARLIEST_BMI_DATE,
    LATEST_BMI_DATE,
    LATEST_VALID_BMI_DATE,
    LATEST_VALID_BMI_VALUE,
    LATEST_ETHNICITY_DATE,
    LATEST_BAME_DATE,
    ALL_BMI_CONCEPT_CODES,
    ALL_BMI_CONCEPT_DISPLAYS,
    ALL_ETHNICITY_CONCEPT_CODES,
    ALL_ETHNICITY_CONCEPT_DISPLAYS
FROM FilteredByAge
WHERE (IS_BMI_30_PLUS OR (IS_BAME AND IS_BMI_27_5_PLUS)); -- Only include patients on the obesity register 