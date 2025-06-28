CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest smoking status recording
    CONCEPT_CODE VARCHAR, -- The concept code for the observation
    CODE_DESCRIPTION VARCHAR, -- The display term for the concept code
    SOURCE_CLUSTER_ID VARCHAR, -- Either SMOK_COD (general smoking codes), LSMOK_COD (current smoker codes), EXSMOK_COD (ex-smoker codes), or NSMOK_COD (never smoked codes)
    IS_SMOKER_CODE BOOLEAN, -- Flag indicating if this is a current smoker code (LSMOK_COD)
    IS_EX_SMOKER_CODE BOOLEAN, -- Flag indicating if this is an ex-smoker code (EXSMOK_COD)
    IS_NEVER_SMOKED_CODE BOOLEAN, -- Flag indicating if this is a never smoked code (NSMOK_COD)
    SMOKING_STATUS VARCHAR -- Derived smoking status (Current Smoker, Ex-Smoker, Never Smoked, Unknown)
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing only the most recent smoking status for each person, derived from INTERMEDIATE_SMOKING_ALL. Uses QOF definitions and cluster IDs: SMOK_COD (general smoking codes), LSMOK_COD (current smoker codes), EXSMOK_COD (ex-smoker codes), and NSMOK_COD (never smoked codes).'
AS
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    OBSERVATION_ID,
    CLINICAL_EFFECTIVE_DATE,
    CONCEPT_CODE,
    CODE_DESCRIPTION,
    SOURCE_CLUSTER_ID,
    IS_SMOKER_CODE,
    IS_EX_SMOKER_CODE,
    IS_NEVER_SMOKED_CODE,
    -- Derive smoking status based on the latest code type
    CASE
        WHEN IS_SMOKER_CODE THEN 'Current Smoker'
        WHEN IS_EX_SMOKER_CODE THEN 'Ex-Smoker'
        WHEN IS_NEVER_SMOKED_CODE THEN 'Never Smoked'
        ELSE 'Unknown'
    END as SMOKING_STATUS
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_SMOKING_ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1;
