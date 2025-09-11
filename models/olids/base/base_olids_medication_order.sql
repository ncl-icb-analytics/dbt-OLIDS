{{
    config(
        secure=true,
        alias='medication_order')
}}

/*
Base MEDICATION_ORDER View
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
    src."medication_statement_id",
    src."encounter_id",
    src."practitioner_id",
    src."observation_id",
    src."allergy_intolerance_id",
    src."diagnostic_order_id",
    src."referral_request_id",
    src."clinical_effective_date",
    src."date_precision_concept_id",
    src."dose",
    src."quantity_value",
    src."quantity_unit",
    src."duration_days",
    src."estimated_cost",
    src."medication_name",
    src."medication_order_source_concept_id",
    src."bnf_reference",
    src."age_at_event",
    src."age_at_event_baby",
    src."age_at_event_neonate",
    src."issue_method",
    src."date_recorded",
    src."is_confidential",
    src."issue_method_description",
    src."lds_start_date_time",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'MEDICATION_ORDER') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code