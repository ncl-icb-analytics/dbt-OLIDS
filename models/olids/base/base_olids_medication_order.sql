{{
    config(
        secure=true,
        alias='medication_order')
}}

/*
Base MEDICATION_ORDER View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Uses native person_id from source table.
Simplified concept mapping using CONCEPT_MAP columns directly.
*/

SELECT
    src.lds_record_id,
    src.id,
    src.organisation_id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.patient_id,
    src.medication_statement_id,
    src.encounter_id,
    src.practitioner_id,
    src.observation_id,
    src.allergy_intolerance_id,
    src.diagnostic_order_id,
    src.referral_request_id,
    src.clinical_effective_date,
    src.date_precision_concept_id,
    date_precision_map.source_code AS date_precision_source_code,
    date_precision_map.source_display AS date_precision_source_display,
    date_precision_map.target_code AS date_precision_code,
    date_precision_map.target_display AS date_precision_display,
    src.dose,
    src.quantity_value,
    src.quantity_unit,
    src.duration_days,
    src.estimated_cost,
    src.medication_name,
    src.medication_order_source_concept_id,
    ms.medication_statement_source_concept_id,
    ms.medication_name AS statement_medication_name,
    concept_map.target_code_id AS mapped_concept_id,
    concept_map.target_code AS mapped_concept_code,
    concept_map.target_display AS mapped_concept_display,
    concept_map.source_code AS source_code,
    concept_map.source_display AS source_display,
    concept_map.source_system AS source_system,
    concept_map.target_system AS target_system,
    bnf.bnf_chapter AS bnf_chapter,
    bnf.bnf_section AS bnf_section,
    bnf.bnf_code AS bnf_code,
    bnf.bnf_name AS bnf_name,
    src.bnf_reference,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.issue_method,
    src.date_recorded,
    src.is_confidential,
    src.issue_method_description,
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
FROM {{ source('olids_common', 'MEDICATION_ORDER') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.record_owner_organisation_code = wnl_practices.practice_code
LEFT JOIN {{ source('olids_common', 'MEDICATION_STATEMENT') }} ms
    ON src.medication_statement_id = ms.id
LEFT JOIN {{ ref('int_enriched_concept_map') }} concept_map
    ON src.medication_order_source_concept_id = concept_map.source_code_id
LEFT JOIN {{ ref('int_enriched_concept_map') }} date_precision_map
    ON src.date_precision_concept_id = date_precision_map.source_code_id
LEFT JOIN DATA_LAB_OLIDS_NCL.REFERENCE.BNF_LATEST bnf
    ON concept_map.target_code = bnf.snomed_code
WHERE src.medication_order_source_concept_id IS NOT NULL
    AND src.lds_start_date_time IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.id ORDER BY concept_map.target_display NULLS LAST) = 1