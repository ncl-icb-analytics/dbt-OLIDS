create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS(
	SOURCE_CODE_ID,
	CONCEPT_ID,
	CONCEPT_SYSTEM,
	CONCEPT_CODE,
	CONCEPT_DISPLAY,
	CLUSTER_ID,
	CLUSTER_DESCRIPTION,
	CODE_DESCRIPTION,
	SOURCE
) target_lag = '4 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 as
-- Map source concept IDs to target concepts and enrich with combined codeset details
SELECT
    MAP."source_code_id"    as SOURCE_CODE_ID,    -- The ID from the source table (e.g., observation_core_concept_id)
    CON."id"                AS CONCEPT_ID,        -- The target Concept ID
    CON."system"            AS CONCEPT_SYSTEM,    -- The Concept's coding system
    CON."code"              AS CONCEPT_CODE,      -- The Concept's code value (usually SNOMED)
    CON."display"           AS CONCEPT_DISPLAY,   -- The Concept's description
    CCS.CLUSTER_ID          AS CLUSTER_ID,        -- Cluster ID from the combined codesets DT
    CCS.CLUSTER_DESCRIPTION AS CLUSTER_DESCRIPTION, -- Cluster Description from the combined codesets DT
    CCS.CODE_DESCRIPTION    AS CODE_DESCRIPTION,  -- Code Description from the combined codesets DT
    CCS.SOURCE              AS SOURCE             -- Source table identifier from the combined codesets DT
FROM
    "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP AS MAP
-- Join to get the target concept details (code, display, system)
-- Using INNER JOIN here assumes every target_code_id in CONCEPT_MAP exists in CONCEPT
JOIN 
    "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT AS CON
    ON MAP."target_code_id" = CON."id"
-- Use LEFT JOIN to include all mappings, even if the concept code isn't in COMBINED_CODESETS
LEFT JOIN 
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.COMBINED_CODESETS AS CCS
    -- Join based on the concept code matching the code in the combined table
    -- Ensure both sides are VARCHAR for reliable joining
    ON CAST(CON."code" AS VARCHAR) = CAST(CCS.CODE AS VARCHAR);