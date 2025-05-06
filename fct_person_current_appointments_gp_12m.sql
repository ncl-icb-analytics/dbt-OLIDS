create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_APPOINTMENTS_GP_12M(
	LDS_PERSON_ID,
	SK_PATIENT_ID,
	MEASURE_ID,
	VALUE,
	ORGANISATION_ID
) target_lag = '4 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 as
SELECT 
    a."person_id" AS LDS_PERSON_ID,
    p."sk_patient_id" AS SK_PATIENT_ID,
    'LDS_PERSON_CURRENT_GP_APPOINTMENTS_12M' AS MEASURE_ID,
    TO_VARIANT(COUNT(a."id")) AS VALUE,
    a."organisation_id" AS ORGANISATION_ID
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.APPOINTMENT a
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p 
    ON a."patient_id" = p."id"
WHERE a."start_date" >= ADD_MONTHS(CURRENT_DATE(), -12)
GROUP BY 
    a."person_id", 
    p."sk_patient_id",
    a."organisation_id";