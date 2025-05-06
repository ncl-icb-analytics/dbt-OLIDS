create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_ORGANISATION_CURRENT_ACTIVE_PATIENTS(
	LDS_ORGANISATION_ID,
	ODS_CODE,
	MEASURE_ID,
	VALUE
) 
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS

as

SELECT 
    org."id" AS lds_organisation_id,
    org."organisation_code" AS ods_code,
    'LDS_ORGANISATION_CURRENT_ACTIVE_LIST_SIZE' AS measure_id,
    COUNT(p."id") AS value
FROM 
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION org
JOIN 
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p ON org."organisation_code" = p."record_owner_organisation_code"
WHERE 
    org."is_obsolete" = FALSE
    AND p."lds_end_date_time" < p."lds_start_date_time"
    AND p."death_year" IS NULL
    AND p."death_month" IS NULL
GROUP BY 
    org."id",
    org."organisation_code";