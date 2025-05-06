-- ==========================================================================
-- Intermediate Dynamic Table holding only OBSERVATION records that successfully
-- map to a known code in the ETHNICITY_CODES table.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PERSON_ETHNICITY_ALL(
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    CLINICAL_EFFECTIVE_DATE DATE, -- Date the ethnicity observation was recorded
    CONCEPT_ID VARCHAR, -- Concept ID of the mapped ethnicity observation
    SNOMED_CODE VARCHAR, -- SNOMED code (or other code system) of the mapped ethnicity observation
    TERM VARCHAR, -- Term/description associated with the ethnicity code from ETHNICITY_CODES
    ETHNICITY_CATEGORY VARCHAR, -- Broad ethnicity category derived from ETHNICITY_CODES
    ETHNICITY_SUBCATEGORY VARCHAR, -- More specific ethnicity subcategory derived from ETHNICITY_CODES
    ETHNICITY_GRANULAR VARCHAR, -- Most granular ethnicity detail available from ETHNICITY_CODES
    OBSERVATION_LDS_ID VARCHAR -- Original LDS ID of the observation record for traceability
)
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Intermediate table containing all dated Observation records that map to a valid code in the ETHNICITY_CODES reference table. Used as a source for DIM_PERSON_ETHNICITY to find the latest valid record per person.'
AS
-- Selects ethnicity-related observations that successfully map to a known code in the ETHNICITY_CODES reference table.
SELECT
    PP."person_id" AS person_id,
    P."sk_patient_id" as sk_patient_id,
    O."clinical_effective_date"::DATE AS clinical_effective_date, -- Cast date
    CON."id" AS concept_id,
    CON."code" AS snomed_code, -- This is the mapped code (e.g., SNOMED)
    E."TERM" AS term, -- Term from the ETHNICITY_CODES reference table
    E."CATEGORY" AS ethnicity_category, -- Category from the ETHNICITY_CODES reference table
    E."SUBCATEGORY" AS ethnicity_subcategory, -- Subcategory from the ETHNICITY_CODES reference table
    E."GRANULAR" AS ethnicity_granular, -- Granular detail from the ETHNICITY_CODES reference table
    O."lds_id" AS observation_lds_id -- Renamed source observation ID for clarity
FROM
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
INNER JOIN -- Link Observation to Person via PATIENT_PERSON.
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
    ON O."patient_id" = PP."patient_id"
INNER JOIN -- Link Observation to Patient to get SK_PATIENT_ID.
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
    ON O."patient_id" = P."id"
INNER JOIN -- Map the observation's core concept ID using CONCEPT_MAP.
    "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP AS MAP
    ON O."observation_core_concept_id" = MAP."source_code_id"
INNER JOIN -- Get details of the mapped target concept (e.g., SNOMED code) from CONCEPT.
    "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS CON
    ON MAP."target_code_id" = CON."id"
-- Crucial INNER JOIN: Ensures that the mapped concept's code exists in the ETHNICITY_CODES reference table.
-- This acts as the primary filter, keeping only valid, recognised ethnicity observations.
INNER JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.ETHNICITY_CODES AS E
    ON E."CODE" = CON."code" -- Compares the mapped code (e.g., SNOMED) to the codes in the reference table.
WHERE
    O."observation_core_concept_id" IS NOT NULL -- Ensures the original observation had a concept ID to map from.
    AND O."clinical_effective_date" IS NOT NULL; -- Ensures the ethnicity observation has a valid date.

