CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_EGFR_LATEST (
    person_id,
    sk_patient_id,
    clinical_effective_date,
    result_value,
    CONCEPT_CODE,
    CODE_DESCRIPTION,
    OBSERVATION_ID
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Select all columns from the EGFR_ALL table.
SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date,
    result_value,
    concept_code,
    code_description,
    observation_id
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_EGFR_ALL -- Source from the EGFR_ALL table
-- Use QUALIFY to filter for the latest record per person.
QUALIFY ROW_NUMBER() OVER (
    -- Partition by person to rank records within each person's history.
    PARTITION BY person_id
    -- Order by date descending (latest first).
    -- Use observation_id descending as a tie-breaker for records on the same date.
    ORDER BY clinical_effective_date DESC, observation_id DESC
) = 1; -- Keep only the row ranked #1 (the latest).