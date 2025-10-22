{{
    config(
        secure=true,
        alias='medication_statement')
}}

/*
Base MEDICATION_STATEMENT View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src.lds_record_id,
    src.id,
    src.organisation_id,
    pp.person_id,
    src.patient_id,
    src.encounter_id,
    src.practitioner_id,
    src.observation_id,
    src.allergy_intolerance_id,
    src.diagnostic_order_id,
    src.referral_request_id,
    src.authorisation_type_concept_id,
    auth_concept.code AS authorisation_type_code,
    auth_concept.display AS authorisation_type_display,
    src.date_precision_concept_id,
    src.medication_statement_source_concept_id,
    mapped_concept.id AS mapped_concept_id,
    mapped_concept.code AS mapped_concept_code,
    mapped_concept.display AS mapped_concept_display,
    src.clinical_effective_date,
    src.cancellation_date,
    src.dose,
    src.quantity_value_description,
    src.quantity_value,
    src.quantity_unit,
    src.medication_name,
    src.bnf_reference,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.issue_method,
    src.date_recorded,
    src.is_active,
    src.is_confidential,
    src.expiry_date,
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
FROM {{ source('olids_common', 'MEDICATION_STATEMENT') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src.patient_id = pp.patient_id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src.record_owner_organisation_code = ncl_practices.practice_code
LEFT JOIN {{ ref('base_olids_concept_map') }} concept_map
    ON src.medication_statement_source_concept_id = concept_map.source_code_id
LEFT JOIN {{ ref('base_olids_concept') }} mapped_concept
    ON concept_map.target_code_id = mapped_concept.id
LEFT JOIN {{ ref('base_olids_concept') }} auth_concept
    ON src.authorisation_type_concept_id = auth_concept.id
WHERE src.medication_statement_source_concept_id IS NOT NULL
    AND src.lds_start_date_time IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.id ORDER BY mapped_concept.display NULLS LAST, auth_concept.display NULLS LAST) = 1