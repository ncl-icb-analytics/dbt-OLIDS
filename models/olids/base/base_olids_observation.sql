{{
    config(
        secure=true,
        alias='observation')
}}

/*
Base OBSERVATION View
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
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_start_date_time",
    src."record_owner_organisation_code",
    src."patient_id",
    src."person_id",
    src."encounter_id",
    src."practitioner_id",
    src."parent_observation_id",
    src."clinical_effective_date",
    src."date_precision_concept_id",
    src."result_value",
    src."result_value_unit_concept_id",
    src."result_date",
    src."result_text",
    src."is_problem",
    src."is_review",
    src."problem_end_date",
    src."observation_source_concept_id",
    src."age_at_event",
    src."age_at_event_baby",
    src."age_at_event_neonate",
    src."episodicity_concept_id",
    src."is_primary",
    src."date_recorded",
    src."is_problem_deleted",
    src."is_confidential"
FROM {{ source('olids_core', 'OBSERVATION') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code