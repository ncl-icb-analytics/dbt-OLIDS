CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_HBA1C_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest HbA1c test
    RESULT_VALUE NUMBER, -- The numeric result value of the latest HbA1c test
    CONCEPT_CODE VARCHAR, -- The concept code associated with the latest HbA1c test
    CODE_DESCRIPTION VARCHAR, -- The description of the concept code for the latest HbA1c test
    IS_IFCC BOOLEAN, -- Flag indicating if this is an IFCC measurement
    IS_DCCT BOOLEAN -- Flag indicating if this is a DCCT measurement
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing only the single most recent HbA1c result for each person, derived from INTERMEDIATE_HBA1C_ALL. Prioritises IFCC measurements over DCCT measurements when dates are equal.'
AS
-- Selects all columns from the INTERMEDIATE_HBA1C_ALL table.
SELECT
    person_id,
    sk_patient_id,
    clinical_effective_date,
    result_value,
    concept_code,
    code_description,
    is_ifcc,
    is_dcct
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_HBA1C_ALL
-- Uses QUALIFY with ROW_NUMBER() to filter for the latest record per person.
-- Prioritises IFCC measurements over DCCT measurements when dates are equal.
QUALIFY ROW_NUMBER() OVER (
    -- Partitions the data by person, so ranking is done independently for each individual.
    PARTITION BY person_id
    -- Orders records within each partition by date (most recent first) and then by measurement type (IFCC preferred over DCCT).
    ORDER BY clinical_effective_date DESC, is_ifcc DESC
) = 1; -- Keeps only the row ranked #1 (i.e., the latest record) for each person. 