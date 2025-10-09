-- Staging model for olids_core.EPISODE_OF_CARE
-- Base layer: base_olids_episode_of_care (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    organisation_id,
    patient_id,
    person_id,
    episode_type_source_concept_id,
    episode_status_source_concept_id,
    episode_of_care_start_date,
    episode_of_care_end_date,
    care_manager_practitioner_id,
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
from {{ ref('base_olids_episode_of_care') }}
