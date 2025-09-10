-- Staging model for olids_core.EPISODE_OF_CARE
-- Source: "Data_Store_OLIDS_Alpha"."OLIDS_MASKED"
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
    "organisation_id" as organisation_id,
    "patient_id" as patient_id,
    "person_id" as person_id,
    "episode_type_source_concept_id" as episode_type_source_concept_id,
    "episode_status_source_concept_id" as episode_status_source_concept_id,
    "episode_of_care_start_date" as episode_of_care_start_date,
    "episode_of_care_end_date" as episode_of_care_end_date,
    "care_manager_practitioner_id" as care_manager_practitioner_id
from {{ source('olids_core', 'EPISODE_OF_CARE') }}
