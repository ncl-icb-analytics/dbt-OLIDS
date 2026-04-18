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
    src.lds_record_id,
    src.id,
    src.nhs_number_hash,
    src.sk_patient_id,
    src.title,
    src.gender_concept_id,
    gender_map.source_code AS gender_source_code,
    gender_map.source_display AS gender_source_display,
    gender_map.target_code AS gender_code,
    gender_map.target_display AS gender_display,
    src.registered_practice_id,
    src.birth_year,
    src.birth_month,
    src.death_year,
    src.death_month,
    src.is_confidential,
    src.is_dummy_patient,
    src.is_spine_sensitive,
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
FROM {{ source('olids_masked', 'PATIENT') }} src
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.record_owner_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} gender_map
    ON src.gender_concept_id = gender_map.source_code_id
WHERE src.sk_patient_id IS NOT NULL
    AND src.is_spine_sensitive = FALSE
    AND src.is_confidential = FALSE
    AND src.is_dummy_patient = FALSE