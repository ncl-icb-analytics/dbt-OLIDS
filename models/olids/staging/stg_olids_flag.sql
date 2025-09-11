-- Staging model for olids_core.FLAG
-- Base layer: base_olids_flag (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "effective_date" as effective_date,
    "expired_date" as expired_date,
    "is_active" as is_active,
    "flag_text" as flag_text
from {{ ref('base_olids_flag') }}
