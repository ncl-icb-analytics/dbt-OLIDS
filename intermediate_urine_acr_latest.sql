CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_LATEST
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'

AS

SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date,
    result_value,
    concept_code,
    code_description,
    observation_id
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_ALL

QUALIFY ROW_NUMBER() OVER (
    -- Partition the data by person so numbering restarts for each person.
    PARTITION BY person_id
    -- Order by date descending to get the latest record first.
    -- Use observation_id descending as a tie-breaker for records on the same date.
    ORDER BY clinical_effective_date DESC, observation_id DESC
) = 1; -- Keep only the row ranked as 1 (the latest) for each person.


