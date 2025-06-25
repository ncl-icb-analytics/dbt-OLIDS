create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_ORGANISATION_ACTIVE_PATIENTS(
	LDS_ORGANISATION_ID VARCHAR, -- Unique identifier for the organisation from the source system
	ODS_CODE VARCHAR, -- ODS (Organisation Data Service) code for the organisation
	MEASURE_ID VARCHAR, -- Identifier for the measure being calculated (fixed as 'LDS_ORGANISATION_ACTIVE_LIST_SIZE')
	VALUE NUMBER -- The calculated count of currently active patients for the organisation
)
COMMENT = 'Fact table providing the current active patient list size for each non-obsolete organisation. Active patients are defined based on specific criteria including lds_end_date_time, lds_start_date_time, and death status.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS

as
-- Calculates the count of currently active patients for each non-obsolete organisation.
SELECT
    org."id" AS lds_organisation_id, -- Source system identifier for the organisation
    org."organisation_code" AS ods_code, -- ODS code of the organisation
    'LDS_ORGANISATION_ACTIVE_LIST_SIZE' AS measure_id, -- Static measure ID indicating the metric being calculated
    COUNT(p."id") AS value -- Counts the number of distinct patient IDs meeting the active criteria for the organisation
FROM
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.ORGANISATION org
JOIN -- Joins ORGANISATION to PATIENT based on the record_owner_organisation_code to link patients to their owning organisation.
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p ON org."organisation_code" = p."record_owner_organisation_code"
WHERE
    org."is_obsolete" = FALSE -- Includes only organisations that are not marked as obsolete.
    AND p."lds_end_date_time" < p."lds_start_date_time" -- This condition seems unusual for active patients. Typically, lds_end_date_time would be NULL or >= lds_start_date_time for an active record. This might be specific logic for this source or an error.
    AND p."death_year" IS NULL -- Excludes patients with a recorded death year.
    AND p."death_month" IS NULL -- Excludes patients with a recorded death month (further check for death status).
GROUP BY -- Groups the results by organisation to count patients per organisation.
    org."id",
    org."organisation_code";
