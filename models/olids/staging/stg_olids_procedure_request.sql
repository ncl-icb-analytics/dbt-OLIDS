-- Staging model for olids_core.PROCEDURE_REQUEST
-- Source: "Data_Store_OLIDS_Alpha"."OLIDS_MASKED"
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
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "encounter_id" as encounter_id,
    "practitioner_id" as practitioner_id,
    "clinical_effective_date" as clinical_effective_date,
    "date_precision_concept_id" as date_precision_concept_id,
    "date_recorded" as date_recorded,
    "description" as description,
    "procedure_source_concept_id" as procedure_source_concept_id,
    "status_concept_id" as status_concept_id,
    "age_at_event" as age_at_event,
    "age_at_event_baby" as age_at_event_baby,
    "age_at_event_neonate" as age_at_event_neonate,
    "is_confidential" as is_confidential,
    "lds_end_date_time" as lds_end_date_time
from {{ source('olids_core', 'PROCEDURE_REQUEST') }}
