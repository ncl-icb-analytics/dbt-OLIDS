-- Staging model for olids_core.PATIENT_CONTACT
-- Base layer: base_olids_patient_contact (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
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
from {{ ref('base_olids_patient_contact') }}
