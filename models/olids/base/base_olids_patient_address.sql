{{
    config(
        secure=true,
        alias='patient_address')
}}

/*
Base PATIENT_ADDRESS View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."patient_id" AS patient_id,
    src."address_type_concept_id" AS address_type_concept_id,
    src."postcode_hash" AS postcode_hash,
    src."start_date" AS start_date,
    src."end_date" AS end_date,
    pp.person_id AS person_id,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."lds_cdm_event_id" AS lds_cdm_event_id,
    src."lds_versioner_event_id" AS lds_versioner_event_id,
    src."record_owner_organisation_code" AS record_owner_organisation_code,
    src."lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    src."lds_initial_date_received_date" AS lds_initial_date_received_date,
    src."lds_start_date_time" AS lds_start_date_time,
    src."lds_lakehouse_date_processed" AS lds_lakehouse_date_processed,
    src."lds_lakehouse_datetime_updated" AS lds_lakehouse_datetime_updated
FROM {{ source('olids_core', 'PATIENT_ADDRESS') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src."patient_id" = pp.patient_id
INNER JOIN {{ ref('base_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code