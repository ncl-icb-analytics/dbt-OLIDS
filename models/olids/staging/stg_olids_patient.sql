-- Staging model for olids_core.PATIENT
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "nhs_number_hash" as nhs_number_hash,
    "sk_patient_id" as sk_patient_id,
    "title" as title,
    "gender_concept_id" as gender_concept_id,
    "registered_practice_id" as registered_practice_id,
    "birth_year" as birth_year,
    "birth_month" as birth_month,
    "death_year" as death_year,
    "death_month" as death_month,
    "is_confidential" as is_confidential,
    "is_dummy_patient" as is_dummy_patient,
    "is_spine_sensitive" as is_spine_sensitive,
    "lds_end_date_time" as lds_end_date_time
from {{ source('olids_core', 'PATIENT') }}
