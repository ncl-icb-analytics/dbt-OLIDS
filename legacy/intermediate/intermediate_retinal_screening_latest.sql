CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_RETINAL_SCREENING_LATEST(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest diabetes retinal screening
    CONCEPT_CODE VARCHAR, -- The specific concept code associated with the screening
    CODE_DESCRIPTION VARCHAR -- The textual description of the concept code
)
TARGET_LAG = '4 hours'
WAREHOUSE = 'NCL_ANALYTICS_XS'
COMMENT = 'Intermediate table containing only the most recent diabetes retinal screening programme completion for each person, derived from INTERMEDIATE_RETINAL_SCREENING_ALL. Only includes completed screenings (does not include declined, unsuitable, or referral codes).'
AS
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    CLINICAL_EFFECTIVE_DATE,
    CONCEPT_CODE,
    CODE_DESCRIPTION
FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_RETINAL_SCREENING_ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1;
