-- Staging model for olids_core.PRACTITIONER_IN_ROLE
-- Base layer: base_olids_practitioner_in_role (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    practitioner_id,
    organisation_id,
    role_code,
    role,
    date_employment_start,
    date_employment_end,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    lds_cdm_event_id,
    lds_versioner_event_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,
    lds_is_deleted,
    lds_start_date_time,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_practitioner_in_role') }}
