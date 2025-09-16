-- Staging model for olids_core.PATIENT_UPRN
-- Base layer: base_olids_patient_uprn (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    registrar_event_id,
    masked_uprn,
    masked_usrn,
    masked_postcode,
    address_format_quality,
    post_code_quality,
    matched_with_assign,
    qualifier,
    uprn_property_classification,
    algorithm,
    match_pattern,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    lds_cdm_event_id,
    lds_registrar_event_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,
    lds_is_deleted,
    lds_start_date_time,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_patient_uprn') }}
