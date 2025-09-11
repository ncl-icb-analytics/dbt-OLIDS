{{
    config(
        secure=true,
        alias='allergy_intolerance')
}}

/*
Base ALLERGY_INTOLERANCE View
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
    src."patient_id",
    src."practitioner_id",
    src."encounter_id",
    src."clinical_status",
    src."verification_status",
    src."category",
    src."clinical_effective_date",
    src."date_precision_concept_id",
    src."is_review",
    src."medication_name",
    src."multi_lex_action",
    src."allergy_intolerance_source_concept_id",
    src."age_at_event",
    src."age_at_event_baby",
    src."age_at_event_neonate",
    src."date_recorded",
    src."is_confidential",
    src."person_id"
FROM {{ source('olids_core', 'ALLERGY_INTOLERANCE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code