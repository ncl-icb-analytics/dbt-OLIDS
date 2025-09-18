{{
    config(
        secure=true,
        alias='patient')
}}

/*
Base Filtered Patient View
Filters out sensitive patients and restricts to NCL practices only.
Used as the foundation for all patient-related clinical data filtering.

Exclusions:
- Patients with is_spine_sensitive = TRUE
- Patients with is_confidential = TRUE
- Patients from non-NCL practices (where STPCode != 'QMJ')
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."nhs_number_hash" AS nhs_number_hash,
    src."sk_patient_id" AS sk_patient_id,
    src."title" AS title,
    src."gender_concept_id" AS gender_concept_id,
    src."registered_practice_id" AS registered_practice_id,
    src."birth_year" AS birth_year,
    src."birth_month" AS birth_month,
    src."death_year" AS death_year,
    src."death_month" AS death_month,
    src."is_confidential" AS is_confidential,
    src."is_dummy_patient" AS is_dummy_patient,
    src."is_spine_sensitive" AS is_spine_sensitive,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."lds_cdm_event_id" AS lds_cdm_event_id,
    src."lds_versioner_event_id" AS lds_versioner_event_id,
    src."record_owner_organisation_code" AS record_owner_organisation_code,
    src."lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    src."lds_initial_date_received_date" AS lds_initial_date_received_date,
    src."lds_is_deleted" AS lds_is_deleted,
    src."lds_start_date_time" AS lds_start_date_time,
    src."lds_lakehouse_date_processed" AS lds_lakehouse_date_processed,
    src."lds_lakehouse_datetime_updated" AS lds_lakehouse_datetime_updated
FROM {{ source('olids_core', 'PATIENT') }} src
INNER JOIN {{ ref('base_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
WHERE src."is_spine_sensitive" = FALSE
    AND src."is_confidential" = FALSE
    AND src."is_dummy_patient" = FALSE