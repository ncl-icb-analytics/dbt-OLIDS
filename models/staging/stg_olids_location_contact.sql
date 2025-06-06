-- Staging model for OLIDS_MASKED.LOCATION_CONTACT
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "location_id" as location_id,
    "is_primary_contact" as is_primary_contact,
    "ldsbusinessid_contacttype" as ldsbusinessid_contacttype,
    "contact_type_concept_id" as contact_type_concept_id,
    "value" as value,
    "lds_end_date_time" as lds_end_date_time
from {{ source('OLIDS_MASKED', 'LOCATION_CONTACT') }}
