-- Staging model for olids_core.PRACTITIONER_IN_ROLE
-- Base layer: base_olids_practitioner_in_role (filtered for NCL practices, excludes sensitive patients)
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
    "practitioner_id" as practitioner_id,
    "organisation_id" as organisation_id,
    "role_code" as role_code,
    "role" as role,
    "date_employment_start" as date_employment_start,
    "date_employment_end" as date_employment_end
from {{ ref('base_olids_practitioner_in_role') }}
