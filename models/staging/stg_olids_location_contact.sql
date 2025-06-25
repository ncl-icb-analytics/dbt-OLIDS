-- Staging model for OLIDS_MASKED.LOCATION_CONTACT
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "location_id" AS location_id,
    "is_primary_contact" AS is_primary_contact,
    "ldsbusinessid_contacttype" AS ldsbusinessid_contacttype,
    "contact_type_concept_id" AS contact_type_concept_id,
    "value" AS value,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'LOCATION_CONTACT') }}
