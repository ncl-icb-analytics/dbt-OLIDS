-- Staging model for olids_core.PATIENT
-- Base layer: base_olids_patient (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    nhs_number_hash,
    sk_patient_id,
    title,
    gender_concept_id,
    registered_practice_id,
    birth_year,
    birth_month,
    death_year,
    death_month,
    is_confidential,
    is_dummy_patient,
    is_spine_sensitive,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    lds_cdm_event_id,
    lds_versioner_event_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_date_received_date,
    lds_is_deleted,
    lds_start_date_time,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('stable_patient') }}
