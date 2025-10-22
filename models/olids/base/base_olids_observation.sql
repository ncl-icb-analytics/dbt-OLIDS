{{
    config(
        secure=true,
        alias='observation')
}}

/*
Base OBSERVATION View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src.lds_record_id,
    src.id,
    src.patient_id,
    pp.person_id,
    src.encounter_id,
    src.practitioner_id,
    src.parent_observation_id,
    src.clinical_effective_date,
    src.date_precision_concept_id,
    src.result_value,
    src.result_value_units_concept_id,
    unit_concept.code AS result_unit_code,
    unit_concept.display AS result_unit_display,
    src.result_date,
    src.result_text,
    src.is_problem,
    src.is_review,
    src.problem_end_date,
    src.observation_source_concept_id,
    mapped_concept.id AS mapped_concept_id,
    mapped_concept.code AS mapped_concept_code,
    mapped_concept.display AS mapped_concept_display,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.episodicity_concept_id,
    src.is_primary,
    src.date_recorded,
    src.is_problem_deleted,
    src.is_confidential,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.record_owner_organisation_code,
    src.lds_datetime_data_acquired,
    src.lds_initial_data_received_date,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'OBSERVATION') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src.patient_id = pp.patient_id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src.record_owner_organisation_code = ncl_practices.practice_code
LEFT JOIN {{ ref('base_olids_concept_map') }} concept_map
    ON src.observation_source_concept_id = concept_map.source_code_id
LEFT JOIN {{ ref('base_olids_concept') }} mapped_concept
    ON concept_map.target_code_id = mapped_concept.id
LEFT JOIN {{ ref('base_olids_concept') }} unit_concept
    ON src.result_value_units_concept_id = unit_concept.id
WHERE src.observation_source_concept_id IS NOT NULL
    AND src.lds_start_date_time IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.id ORDER BY mapped_concept.display NULLS LAST, unit_concept.display NULLS LAST) = 1