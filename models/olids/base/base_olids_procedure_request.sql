{{
    config(
        secure=true,
        alias='procedure_request')
}}

/*
Base PROCEDURE_REQUEST View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Uses native person_id from source table.
*/

SELECT
    src.lds_record_id,
    src.id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.patient_id,
    src.encounter_id,
    src.practitioner_id,
    src.clinical_effective_date,
    src.date_precision_concept_id,
    src.date_recorded,
    src.description,
    src.procedure_request_source_concept_id,
    src.status_concept_id,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.is_confidential,
    src.lds_end_date_time,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.record_owner_organisation_code,
    src.lds_datetime_data_acquired,
    src.lds_initial_data_received_date,
    src.lds_is_deleted,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'PROCEDURE_REQUEST') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.record_owner_organisation_code = wnl_practices.practice_code
WHERE src.lds_start_date_time IS NOT NULL