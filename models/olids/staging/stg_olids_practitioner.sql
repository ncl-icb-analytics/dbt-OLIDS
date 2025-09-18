-- Staging model for olids_core.PRACTITIONER
-- Base layer: base_olids_practitioner (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    gmc_code,
    title,
    first_name,
    last_name,
    name,
    is_obsolete,
    lds_end_date_time,
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
from {{ ref('base_olids_practitioner') }}
