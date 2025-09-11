-- Staging model for olids_core.APPOINTMENT_PRACTITIONER
-- Base layer: base_olids_appointment_practitioner (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_record_id_user" as lds_record_id_user,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "appointment_id" as appointment_id,
    "practitioner_id" as practitioner_id,
    "lds_end_date_time" as lds_end_date_time
from {{ ref('base_olids_appointment_practitioner') }}
