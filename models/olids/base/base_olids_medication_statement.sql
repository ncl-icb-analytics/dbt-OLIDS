{{
    config(
        secure=true,
        alias='medication_statement')
}}

/*
Base MEDICATION_STATEMENT View
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
    src."record_owner_organisation_code",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_dataset_id",
    src."organisation_id",
    src."person_id",
    src."patient_id",
    src."encounter_id",
    src."practitioner_id",
    src."observation_id",
    src."allergy_intolerance_id",
    src."diagnostic_order_id",
    src."referral_request_id",
    src."authorisation_type_concept_id",
    src."date_precision_concept_id",
    src."medication_statement_source_concept_id",
    src."clinical_effective_date",
    src."cancellation_date",
    src."dose",
    src."quantity_value_description",
    src."quantity_value",
    src."quantity_unit",
    src."medication_name",
    src."bnf_reference",
    src."age_at_event",
    src."age_at_event_baby",
    src."age_at_event_neonate",
    src."issue_method",
    src."date_recorded",
    src."is_active",
    src."is_confidential",
    src."expiry_date",
    src."lds_start_date_time"
FROM {{ source('olids_core', 'MEDICATION_STATEMENT') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code