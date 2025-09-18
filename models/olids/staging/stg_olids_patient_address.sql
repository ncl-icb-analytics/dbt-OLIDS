-- Staging model for olids_core.PATIENT_ADDRESS
-- Base layer: base_olids_patient_address (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    patient_id,
    address_type_concept_id,
    postcode_hash,
    start_date,
    end_date,
    person_id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    lds_cdm_event_id,
    lds_versioner_event_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_date_received_date,
    lds_start_date_time,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('stable_patient_address') }}
