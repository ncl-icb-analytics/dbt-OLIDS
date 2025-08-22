{{ config(materialized='view') }}

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "organisation_id" as organisation_id,
    "practitioner_id" as practitioner_id,
    "episode_of_care_id" as episode_of_care_id,
    "start_date" as start_date,
    "end_date" as end_date
from {{ source('olids_masked', 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE') }}