create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_SEX(
	PERSON_ID,
	SEX
) target_lag = '4 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='This dimension is currently using hardcoded gender concept ID''s to work around an issue with the Concept Map and Concept tables. Once resolved, this should revert to properly looking up values.'
 as
SELECT DISTINCT -- Use DISTINCT to ensure one row per person if PATIENT has duplicates for a PATIENT_PERSON link
    pp."person_id" AS PERSON_ID,
    -- Hardcode SEX based on known gender_concept_ids
    CASE
        WHEN p."gender_concept_id" = '4907ce31-7168-4385-b91d-a7fe171a1c8f' THEN 'Female' -- Known Female ID
        WHEN p."gender_concept_id" = '3ae10994-efd0-47db-ade4-e440eaf0f973' THEN 'Male'   -- Assumed Male ID
        ELSE 'Unknown' -- Handle NULL or any other unexpected IDs
    END AS SEX
FROM
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
INNER JOIN
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp ON p."id" = pp."patient_id";