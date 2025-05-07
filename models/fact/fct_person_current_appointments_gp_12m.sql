create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_APPOINTMENTS_GP_12M(
	LDS_PERSON_ID VARCHAR, -- Unique identifier for the person from the source system (linked via APPOINTMENT table)
	SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient (linked via PATIENT table)
	MEASURE_ID VARCHAR, -- Identifier for the measure being calculated (fixed as 'LDS_PERSON_CURRENT_GP_APPOINTMENTS_12M')
	VALUE VARIANT, -- The calculated count of GP appointments in the last 12 months for the person (stored as VARIANT)
	ORGANISATION_ID VARCHAR -- Identifier for the organisation associated with the appointment
)
COMMENT = 'Fact table counting the number of GP appointments for each person within the last 12 months. Links appointments to persons and their associated organisation.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS
 as
-- Calculates the total number of GP appointments for each person at each organisation within the last 12 months.
SELECT 
    a."person_id" AS LDS_PERSON_ID, -- Person identifier from the APPOINTMENT table
    p."sk_patient_id" AS SK_PATIENT_ID, -- Surrogate key for the patient from the PATIENT table
    'LDS_PERSON_CURRENT_GP_APPOINTMENTS_12M' AS MEASURE_ID, -- Static measure ID indicating the metric
    TO_VARIANT(COUNT(a."id")) AS VALUE, -- Counts distinct appointments. Stored as VARIANT; might be for future schema flexibility or specific use case.
    a."organisation_id" AS ORGANISATION_ID -- Organisation ID from the APPOINTMENT table
FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.APPOINTMENT a
JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p -- Joins APPOINTMENT to PATIENT to link appointments to patient surrogate keys.
    ON a."patient_id" = p."id"
WHERE a."start_date" >= ADD_MONTHS(CURRENT_DATE(), -12) -- Filters for appointments with a start date within the last 12 months from the current date.
GROUP BY -- Groups results to count appointments per person per organisation.
    a."person_id", 
    p."sk_patient_id",
    a."organisation_id";