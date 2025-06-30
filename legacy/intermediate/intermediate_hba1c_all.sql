CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_HBA1C_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the HbA1c test was performed/recorded
    RESULT_VALUE NUMBER(6,1), -- The numeric result value of the HbA1c test (float, up to 2 decimal places)
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the HbA1c test observation
    CODE_DESCRIPTION VARCHAR, -- The textual description of the concept code
    IS_IFCC BOOLEAN, -- Flag indicating if this is an IFCC measurement
    IS_DCCT BOOLEAN -- Flag indicating if this is a DCCT measurement
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded HbA1c results for all persons. Filters based on IFCCHBAM_COD and DCCTHBA1C_COD concept codes. Excludes records with NULL result values. Includes flags to distinguish between IFCC and DCCT measurements. HbA1c values are stored as floats.'
AS
-- Selects distinct HbA1c observation records.
-- Uses DISTINCT as a precaution against potential duplicate source records.
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date"::DATE as clinical_effective_date, -- Cast to DATE
    CAST(o."result_value" AS NUMBER(6,2)) as result_value,
    c.concept_code,
    c.code_description,
    -- Flag IFCC measurements
    CASE
        WHEN c.cluster_id = 'IFCCHBAM_COD' THEN TRUE
        ELSE FALSE
    END as is_ifcc,
    -- Flag DCCT measurements
    CASE
        WHEN c.cluster_id = 'DCCTHBA1C_COD' THEN TRUE
        ELSE FALSE
    END as is_dcct
-- Source table for observations.
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION O
-- Join to MAPPED_CONCEPTS to filter based on concept codes.
JOIN
    DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS C
    ON O."observation_core_concept_id" = C.SOURCE_CODE_ID
-- Join to link observation patient_id to person_id.
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
-- Join to link observation patient_id to patient surrogate key.
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
-- Filter for specific HbA1c concept codes.
WHERE C.CLUSTER_ID IN ('IFCCHBAM_COD', 'DCCTHBA1C_COD')
-- Filter out records where the result value itself is missing.
AND o."result_value" IS NOT NULL;
