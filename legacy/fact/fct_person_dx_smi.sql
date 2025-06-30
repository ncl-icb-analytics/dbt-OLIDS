CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_DX_SMI (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person
    IS_ON_SMI_REGISTER BOOLEAN, -- Flag indicating if person is on the SMI register
    IS_ON_LITHIUM BOOLEAN, -- Flag indicating if person has a lithium order in the last 6 months and not stopped
    HAS_MH_DIAGNOSIS BOOLEAN, -- Flag indicating if person has a mental health diagnosis
    IS_IN_REMISSION BOOLEAN, -- Flag indicating if person is in remission
    EARLIEST_MH_DIAGNOSIS_DATE DATE, -- Earliest mental health diagnosis date
    LATEST_MH_DIAGNOSIS_DATE DATE, -- Latest mental health diagnosis date
    LATEST_REMISSION_DATE DATE, -- Latest remission date
    LATEST_LITHIUM_ORDER_DATE DATE, -- Latest lithium order date
    LATEST_LITHIUM_STOPPED_DATE DATE, -- Latest lithium stopped date
    ALL_MH_CONCEPT_CODES ARRAY, -- All mental health concept codes for this person
    ALL_MH_CONCEPT_DISPLAYS ARRAY, -- All mental health concept display terms for this person
    ALL_LITHIUM_CONCEPT_CODES ARRAY, -- All lithium concept codes for this person
    ALL_LITHIUM_CONCEPT_DISPLAYS ARRAY -- All lithium concept display terms for this person
)
COMMENT = 'Fact table for Serious Mental Illness (SMI) register. Includes patients who meet either: 1) have a mental health diagnosis that is not in remission, or 2) have been issued a prescription for lithium therapy in the last 6 months (LITHIUM_ISSUED_LAST_6M). Tracks diagnosis dates, remission status, and lithium therapy details including order and stopped dates.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH FilteredByAge AS (
    -- Get all relevant patients with their age
    SELECT
        COALESCE(mh.PERSON_ID, li.PERSON_ID) AS PERSON_ID,
        COALESCE(mh.SK_PATIENT_ID, li.SK_PATIENT_ID) AS SK_PATIENT_ID,
        age.AGE,
        mh.EARLIEST_DIAGNOSIS_DATE AS EARLIEST_MH_DIAGNOSIS_DATE,
        mh.LATEST_DIAGNOSIS_DATE AS LATEST_MH_DIAGNOSIS_DATE,
        mh.LATEST_REMISSION_DATE,
        mh.IS_IN_REMISSION,
        mh.ALL_MH_CONCEPT_CODES,
        mh.ALL_MH_CONCEPT_DISPLAYS,
        li.LATEST_LITHIUM_ORDER_DATE,
        li.LATEST_LITHIUM_STOPPED_DATE,
        li.LITHIUM_ISSUED_LAST_6M,
        li.ALL_LITHIUM_CONCEPT_CODES,
        li.ALL_LITHIUM_CONCEPT_DISPLAYS
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_MH_DIAGNOSES mh
    FULL OUTER JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_LITHIUM_ORDERS li
        ON mh.PERSON_ID = li.PERSON_ID
    JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
        ON COALESCE(mh.PERSON_ID, li.PERSON_ID) = age.PERSON_ID
)
-- Final selection implementing business rules
SELECT
    f.PERSON_ID,
    f.SK_PATIENT_ID,
    f.AGE,
    -- Person is on SMI register if they either:
    -- 1. Have a mental health diagnosis (not in remission)
    -- 2. OR have a lithium order in the last 6 months and not stopped
    CASE
        WHEN (f.EARLIEST_MH_DIAGNOSIS_DATE IS NOT NULL AND NOT f.IS_IN_REMISSION)
            OR f.LITHIUM_ISSUED_LAST_6M THEN TRUE
        ELSE FALSE
    END AS IS_ON_SMI_REGISTER,
    COALESCE(f.LITHIUM_ISSUED_LAST_6M, FALSE) AS IS_ON_LITHIUM,
    f.EARLIEST_MH_DIAGNOSIS_DATE IS NOT NULL AS HAS_MH_DIAGNOSIS,
    COALESCE(f.IS_IN_REMISSION, FALSE) AS IS_IN_REMISSION,
    f.EARLIEST_MH_DIAGNOSIS_DATE,
    f.LATEST_MH_DIAGNOSIS_DATE,
    f.LATEST_REMISSION_DATE,
    f.LATEST_LITHIUM_ORDER_DATE,
    f.LATEST_LITHIUM_STOPPED_DATE,
    f.ALL_MH_CONCEPT_CODES,
    f.ALL_MH_CONCEPT_DISPLAYS,
    f.ALL_LITHIUM_CONCEPT_CODES,
    f.ALL_LITHIUM_CONCEPT_DISPLAYS
FROM FilteredByAge f;
