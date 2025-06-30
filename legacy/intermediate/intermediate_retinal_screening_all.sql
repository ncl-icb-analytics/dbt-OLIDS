CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_RETINAL_SCREENING_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the diabetes retinal screening was performed
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the screening
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing all recorded diabetes retinal screening programme completions. Filters based on RETSCREN_COD concept codes which only indicate completed screenings (does not include declined, unsuitable, or referral codes).'
AS
SELECT DISTINCT
    pp."person_id" as person_id,
    p."sk_patient_id" as sk_patient_id,
    o."clinical_effective_date"::DATE as clinical_effective_date,
    c.concept_code,
    c.code_description
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp ON o."patient_id" = pp."patient_id"
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p ON pp."patient_id" = p."id"
JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS c ON o."observation_core_concept_id" = c.concept_id
WHERE c.CLUSTER_ID = 'RETSCREN_COD'
    AND o."clinical_effective_date" IS NOT NULL;
