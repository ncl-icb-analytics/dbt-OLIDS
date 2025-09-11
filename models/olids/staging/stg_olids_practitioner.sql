-- Staging model for olids_core.PRACTITIONER
-- Base layer: base_olids_practitioner (filtered for NCL practices, excludes sensitive patients)
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
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "gmc_code" as gmc_code,
    "title" as title,
    "first_name" as first_name,
    "last_name" as last_name,
    "name" as name,
    "is_obsolete" as is_obsolete,
    "lds_end_date_time" as lds_end_date_time
from {{ ref('base_olids_practitioner') }}
