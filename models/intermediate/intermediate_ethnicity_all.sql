create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_ETHNICITY_ALL(
	PERSON_ID,
	SK_PATIENT_ID,
	CLINICAL_EFFECTIVE_DATE,
	CONCEPT_ID,
	SNOMED_CODE,
	TERM,
	ETHNICITY_CATEGORY,
	ETHNICITY_SUBCATEGORY,
	ETHNICITY_GRANULAR,
	OBSERVATION_LDS_ID
) target_lag = '4 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Intermediate table containing all dated Observation records that map to a valid code in the ETHNICITY_CODES reference table. Used as a source for DIM_PERSON_ETHNICITY to find the latest valid record per person.'
 as
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
    AND O."clinical_effective_date" IS NOT NULL;