{{ config(
  materialized='view',
  tags=['staging_mv'],
  post_hook=[
    "{% set _rb = var('rebuild_mv', none) %}{% if _rb is true %}create or replace materialized view {{ this.database }}.{{ this.schema }}.stg_olids_medication_order_mv cluster by (medication_order_core_concept_id, clinical_effective_date) as select \"lds_id\" as lds_id, \"id\" as id, \"lds_business_key\" as lds_business_key, \"record_owner_organisation_code\" as record_owner_organisation_code, \"lds_datetime_data_acquired\" as lds_datetime_data_acquired, \"lds_initial_data_received_date\" as lds_initial_data_received_date, \"lds_dataset_id\" as lds_dataset_id, \"organisation_id\" as organisation_id, \"person_id\" as person_id, \"patient_id\" as patient_id, \"medication_statement_id\" as medication_statement_id, \"encounter_id\" as encounter_id, \"practitioner_id\" as practitioner_id, \"observation_id\" as observation_id, \"allergy_intolerance_id\" as allergy_intolerance_id, \"diagnostic_order_id\" as diagnostic_order_id, \"referral_request_id\" as referral_request_id, \"clinical_effective_date\" as clinical_effective_date, \"date_precision_concept_id\" as date_precision_concept_id, \"dose\" as dose, \"quantity_value\" as quantity_value, \"quantity_unit\" as quantity_unit, \"duration_days\" as duration_days, \"estimated_cost\" as estimated_cost, \"medication_name\" as medication_name, \"medication_order_core_concept_id\" as medication_order_core_concept_id, \"bnf_reference\" as bnf_reference, \"age_at_event\" as age_at_event, \"age_at_event_baby\" as age_at_event_baby, \"age_at_event_neonate\" as age_at_event_neonate, \"issue_method\" as issue_method, \"date_recorded\" as date_recorded, \"is_confidential\" as is_confidential, \"lds_start_date_time\" as lds_start_date_time, \"lds_end_date_time\" as lds_end_date_time from {{ source('OLIDS_MASKED', 'MEDICATION_ORDER') }}{% elif _rb is sameas false %}create materialized view if not exists {{ this.database }}.{{ this.schema }}.stg_olids_medication_order_mv cluster by (medication_order_core_concept_id, clinical_effective_date) as select \"lds_id\" as lds_id, \"id\" as id, \"lds_business_key\" as lds_business_key, \"record_owner_organisation_code\" as record_owner_organisation_code, \"lds_datetime_data_acquired\" as lds_datetime_data_acquired, \"lds_initial_data_received_date\" as lds_initial_data_received_date, \"lds_dataset_id\" as lds_dataset_id, \"organisation_id\" as organisation_id, \"person_id\" as person_id, \"patient_id\" as patient_id, \"medication_statement_id\" as medication_statement_id, \"encounter_id\" as encounter_id, \"practitioner_id\" as practitioner_id, \"observation_id\" as observation_id, \"allergy_intolerance_id\" as allergy_intolerance_id, \"diagnostic_order_id\" as diagnostic_order_id, \"referral_request_id\" as referral_request_id, \"clinical_effective_date\" as clinical_effective_date, \"date_precision_concept_id\" as date_precision_concept_id, \"dose\" as dose, \"quantity_value\" as quantity_value, \"quantity_unit\" as quantity_unit, \"duration_days\" as duration_days, \"estimated_cost\" as estimated_cost, \"medication_name\" as medication_name, \"medication_order_core_concept_id\" as medication_order_core_concept_id, \"bnf_reference\" as bnf_reference, \"age_at_event\" as age_at_event, \"age_at_event_baby\" as age_at_event_baby, \"age_at_event_neonate\" as age_at_event_neonate, \"issue_method\" as issue_method, \"date_recorded\" as date_recorded, \"is_confidential\" as is_confidential, \"lds_start_date_time\" as lds_start_date_time, \"lds_end_date_time\" as lds_end_date_time from {{ source('OLIDS_MASKED', 'MEDICATION_ORDER') }}{% endif %}"
  ]
) }}

-- Staging model for OLIDS_MASKED.MEDICATION_ORDER
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_dataset_id" AS lds_dataset_id,
    "organisation_id" AS organisation_id,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "medication_statement_id" AS medication_statement_id,
    "encounter_id" AS encounter_id,
    "practitioner_id" AS practitioner_id,
    "observation_id" AS observation_id,
    "allergy_intolerance_id" AS allergy_intolerance_id,
    "diagnostic_order_id" AS diagnostic_order_id,
    "referral_request_id" AS referral_request_id,
    "clinical_effective_date" AS clinical_effective_date,
    "date_precision_concept_id" AS date_precision_concept_id,
    "dose" AS dose,
    "quantity_value" AS quantity_value,
    "quantity_unit" AS quantity_unit,
    "duration_days" AS duration_days,
    "estimated_cost" AS estimated_cost,
    "medication_name" AS medication_name,
    "medication_order_core_concept_id" AS medication_order_core_concept_id,
    "bnf_reference" AS bnf_reference,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "issue_method" AS issue_method,
    "date_recorded" AS date_recorded,
    "is_confidential" AS is_confidential,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'MEDICATION_ORDER') }}
