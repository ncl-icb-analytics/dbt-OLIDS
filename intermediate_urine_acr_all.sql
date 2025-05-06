CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_URINE_ACR_ALL
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'

AS
-- Select distinct rows to avoid duplicates.
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date" as clinical_effective_date,
    o."result_value" as result_value,
    c.concept_code,
    c.code_description,
    o."id" as observation_id
-- Source table for observations.
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION O

JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS C
    ON O."observation_core_concept_id" = C.SOURCE_CODE_ID

JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    ON o."patient_id" = pp."patient_id"

JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    ON o."patient_id" = p."id"
-- Filter for specific ACR concept codes.
WHERE C.CONCEPT_CODE IN
    ('5801000237100',    -- ACR codes are not properly defined through Clusters 
     '1027791000000103', -- Using hardcoded values until terminology server integration 
     '1023491000000104') 
-- Filter out records where the result value is missing.
AND o."result_value" IS NOT NULL;