{{
    config(
        secure=true,
        alias='encounter')
}}

/*
Base ENCOUNTER View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
*/

SELECT
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_dataset_id",
    src."record_owner_organisation_code",
    src."person_id",
    src."patient_id",
    src."practitioner_id",
    src."appointment_id",
    src."episode_of_care_id",
    src."service_provider_organisation_id",
    src."clinical_effective_date",
    src."date_precision_concept_id",
    src."location",
    src."encounter_source_concept_id",
    src."age_at_event",
    src."age_at_event_baby",
    src."age_at_event_neonate",
    src."type",
    src."sub_type",
    src."admission_method",
    src."end_date",
    src."date_recorded",
    src."lds_start_date_time",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'ENCOUNTER') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code