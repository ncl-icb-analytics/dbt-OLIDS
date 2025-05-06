-- ==========================================================================
-- Intermediate Dynamic Table holding only OBSERVATION records that successfully
-- map to a known code in the ETHNICITY_CODES table.
-- ==========================================================================
CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PERSON_ETHNICITY_ALL
TARGET_LAG = '4 hours'
REFRESH_MODE = auto
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    PP."person_id" AS person_id,
    P."sk_patient_id" as sk_patient_id,
    O."clinical_effective_date" AS clinical_effective_date,
    CON."id" AS concept_id,
    CON."code" AS snomed_code,
    E."TERM" AS term,
    E."CATEGORY" AS ethnicity_category,
    E."SUBCATEGORY" AS ethnicity_subcategory,
    E."GRANULAR" AS ethnicity_granular,
    O."lds_id" AS observation_lds_id -- Renamed for clarity
FROM
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
INNER JOIN -- Use INNER JOIN here as we need patient/person links
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
    ON O."patient_id" = PP."patient_id"
INNER JOIN -- Use INNER JOIN here as we need patient/person links
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" AS P
    ON O."patient_id" = P."id"
INNER JOIN -- Use INNER JOIN as we only care about observations that can be mapped
    "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP AS MAP
    ON O."observation_core_concept_id" = MAP."source_code_id"
INNER JOIN -- Use INNER JOIN as we only care about mapped concepts that exist
    "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS CON
    ON MAP."target_code_id" = CON."id"
-- Use INNER JOIN to ensure the mapped concept exists in the ETHNICITY_CODES table
INNER JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.ETHNICITY_CODES AS E
    ON E."CODE" = CON."code" -- Assuming E."CODE" and CON."code" are comparable types or cast needed
WHERE
    O."observation_core_concept_id" IS NOT NULL -- Ensure the observation has a concept ID
    AND O."clinical_effective_date" IS NOT NULL; -- Ensure the observation has a date

