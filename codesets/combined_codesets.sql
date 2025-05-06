create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.CODESETS.COMBINED_CODESETS(
	CLUSTER_ID,
	CLUSTER_DESCRIPTION,
	CODE,
	CODE_DESCRIPTION,
	SOURCE
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 as
-- PCD data
SELECT 
    cluster_id,
    cluster_description,
    CAST(SNOMED_Code AS VARCHAR) as code,
    SNOMED_Code_Description as code_description,
    'PCD' as source
FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.pcd_refset_latest
UNION ALL
-- UKHSA COVID data
SELECT 
    CLUSTER_ID as cluster_id,
    CLUSTER_DESCRIPTION as cluster_description,
    CAST(SNOMED_Code AS VARCHAR) as code,
    SNOMED_DESCRIPTION as code_description,
    'UKHSA_COVID' as source
FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.UKHSA_COVID_LATEST
UNION ALL
-- UKHSA FLU data
SELECT 
    CODE_GROUP as cluster_id,
    CODE_GROUP_DESCRIPTION as cluster_description,
    CAST(SNOMED_Code AS VARCHAR) as code,
    SNOMED_DESCRIPTION as code_description,
    'UKHSA_FLU' as source
FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.UKHSA_FLU_LATEST;