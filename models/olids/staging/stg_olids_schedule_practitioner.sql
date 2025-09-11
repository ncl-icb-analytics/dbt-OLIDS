-- Staging model for olids_core.SCHEDULE_PRACTITIONER
-- Base layer: base_olids_schedule_practitioner (filtered for NCL practices, excludes sensitive patients)
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
    "lds_start_date_time" as lds_start_date_time,
    "schedule_id" as schedule_id,
    "practitioner_id" as practitioner_id
from {{ ref('base_olids_schedule_practitioner') }}
