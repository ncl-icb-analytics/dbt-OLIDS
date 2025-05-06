CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest ACR test
    RESULT_VALUE NUMBER, -- The numeric result value of the latest Urine ACR test
    CONCEPT_CODE VARCHAR, -- The concept code associated with the latest ACR test
    CODE_DESCRIPTION VARCHAR, -- The description of the concept code for the latest ACR test
    OBSERVATION_ID VARCHAR -- The unique identifier for the latest observation record
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing only the single most recent Urine ACR result for each person, derived from INTERMEDIATE_URINE_ACR_ALL.'
AS
-- Selects all columns from the INTERMEDIATE_URINE_ACR_ALL table.
SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date,
    result_value,
    concept_code,
    code_description,
    observation_id
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_ALL
-- Uses QUALIFY with ROW_NUMBER() to filter for the latest record per person.
QUALIFY ROW_NUMBER() OVER (
    -- Partitions the data by person, so ranking is done independently for each individual.
    PARTITION BY person_id
    -- Orders records within each partition by date (most recent first) and then by observation ID (descending) as a tie-breaker.
    ORDER BY clinical_effective_date DESC, observation_id DESC
) = 1; -- Keeps only the row ranked #1 (i.e., the latest record) for each person.


