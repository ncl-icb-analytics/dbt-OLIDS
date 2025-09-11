{{
    config(
        secure=true,
        alias='episode_of_care')
}}

/*
Base EPISODE_OF_CARE View
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
    src."lds_dataset_id",
    src."record_owner_organisation_code",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_start_date_time",
    src."organisation_id",
    src."patient_id",
    src."person_id",
    src."episode_type_source_concept_id",
    src."episode_status_source_concept_id",
    src."episode_of_care_start_date",
    src."episode_of_care_end_date",
    src."care_manager_practitioner_id"
FROM {{ source('olids_core', 'EPISODE_OF_CARE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code