CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the ACR test was performed/recorded
    RESULT_VALUE NUMBER(6,2), -- The numeric result value of the Urine ACR test (6,2 format)
    RESULT_UNIT_DISPLAY VARCHAR, -- Display value for the result unit (e.g., 'mg/mmol')
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the ACR test observation
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded Urine Albumin-to-Creatinine Ratio (ACR) results for all persons. Filters based on a hardcoded list of relevant ACR concept codes due to current limitations in cluster definitions. Excludes records with NULL result values. Includes the display value for the result unit.'
AS
-- Selects distinct Urine ACR observation records.
-- Uses DISTINCT as a precaution against potential duplicate source records.
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date"::DATE as clinical_effective_date, -- Cast to DATE
    o."result_value" as result_value,
    unit_con."display" as result_unit_display,
    c.concept_code,
    c.code_description
-- Source table for observations.
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION O
-- Join to MAPPED_CONCEPTS to filter based on concept codes.
JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS C
    ON O."observation_core_concept_id" = C.SOURCE_CODE_ID
-- Join to link observation patient_id to person_id.
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"
-- Join to link observation patient_id to patient surrogate key.
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
-- Join to get the result unit display
LEFT JOIN "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT unit_con
    ON o."result_value_unit_concept_id" = unit_con."id"
-- Filter for specific ACR concept codes.
-- NOTE: Uses hardcoded CONCEPT_CODE values because appropriate Clusters are not yet defined or available.
-- This list should be reviewed and potentially updated once terminology mapping is improved.
WHERE C.CONCEPT_CODE IN
    ('5801000237100',
     '1027791000000103',
     '1023491000000104')
-- Filter out records where the result value itself is missing.
AND o."result_value" IS NOT NULL;
