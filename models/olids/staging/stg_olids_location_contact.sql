-- Staging model for olids_core.LOCATION_CONTACT
-- Base layer: base_olids_location_contact (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "location_id" as location_id,
    "is_primary_contact" as is_primary_contact,
    "contact_type" as contact_type,
    "contact_type_concept_id" as contact_type_concept_id,
    "value" as value,
    "lds_end_date_time" as lds_end_date_time
from {{ ref('base_olids_location_contact') }}
