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
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_dataset_id",
    src."lds_datetime_data_acquired",
    src."lds_start_date_time",
    src."record_owner_organisation_code",
    src."nhs_number_hash",
    src."sk_patient_id",
    src."title",
    src."gender_concept_id",
    src."registered_practice_id",
    src."birth_year",
    src."birth_month",
    src."death_year",
    src."death_month",
    src."is_confidential",
    src."is_dummy_patient",
    src."is_spine_sensitive",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'PATIENT') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
WHERE src."is_spine_sensitive" = FALSE
    AND src."is_confidential" = FALSE
    AND src."is_dummy_patient" = FALSE