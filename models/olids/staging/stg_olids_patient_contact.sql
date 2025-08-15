-- Staging model for olids_core.PATIENT_CONTACT
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "lds_start_date_time" as lds_start_date_time,
    "description" as description,
    "contact_type_concept_id" as contact_type_concept_id,
    "start_date" as start_date,
    "end_date" as end_date
from {{ source('olids_core', 'PATIENT_CONTACT') }}
