{{
    config(
        secure=true,
        alias='allergy_intolerance')
}}

/*
Base ALLERGY_INTOLERANCE View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Uses native person_id from source table.
*/

SELECT
    src.lds_record_id,
    src.id,
    src.patient_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.practitioner_id,
    src.encounter_id,
    src.clinical_status,
    src.verification_status,
    src.category,
    src.clinical_effective_date,
    src.date_precision_concept_id,
    date_precision_map.source_code AS date_precision_source_code,
    date_precision_map.source_display AS date_precision_source_display,
    date_precision_map.target_code AS date_precision_code,
    date_precision_map.target_display AS date_precision_display,
    src.is_review,
    src.medication_name,
    src.multi_lex_action,
    src.allergy_intolerance_source_concept_id,
    concept_map.source_code AS source_code,
    concept_map.source_display AS source_display,
    concept_map.source_system AS source_system,
    concept_map.target_code_id AS mapped_concept_id,
    concept_map.target_code AS mapped_concept_code,
    concept_map.target_display AS mapped_concept_display,
    concept_map.target_system AS target_system,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.date_recorded,
    src.is_confidential,
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
FROM {{ source('olids_common', 'ALLERGY_INTOLERANCE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.record_owner_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ ref('int_enriched_concept_map') }} concept_map
    ON src.allergy_intolerance_source_concept_id = concept_map.source_code_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} date_precision_map
    ON src.date_precision_concept_id = date_precision_map.source_code_id
WHERE src.lds_start_date_time IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.id ORDER BY concept_map.target_display NULLS LAST, date_precision_map.target_display NULLS LAST) = 1