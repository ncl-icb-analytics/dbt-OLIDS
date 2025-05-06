create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_SEX(
	PERSON_ID VARCHAR, -- Unique identifier for a person
	SEX VARCHAR -- Derived sex of the person ('Female', 'Male', or 'Unknown')
)
COMMENT='Dimension table for person sex. This dimension is currently using hardcoded gender_concept_id values to determine sex due to issues with Concept Map/Concept tables. This should be updated once the underlying data issues are resolved.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS
 as
-- Selects distinct persons and derives their sex based on hardcoded gender_concept_id values.
-- Uses DISTINCT to ensure a single row per PERSON_ID, in case of multiple patient records linking to the same person.
SELECT DISTINCT 
    pp."person_id" AS PERSON_ID,
    -- Derives SEX by mapping specific gender_concept_id values to 'Female' or 'Male'.
    -- Any other gender_concept_id or a NULL value results in 'Unknown'.
    CASE
        WHEN p."gender_concept_id" = '4907ce31-7168-4385-b91d-a7fe171a1c8f' THEN 'Female' -- Hardcoded ID for Female
        WHEN p."gender_concept_id" = '3ae10994-efd0-47db-ade4-e440eaf0f973' THEN 'Male'   -- Hardcoded ID for Male
        ELSE 'Unknown' -- Default for NULL or any other gender_concept_id values
    END AS SEX
FROM
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
INNER JOIN
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp ON p."id" = pp."patient_id";