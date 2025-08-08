{{ config(
  materialized='view',
  tags=['staging_mv'],
  post_hook=[
    "{% set _rb = var('rebuild_mv', none) %}{% if _rb is true %}create or replace materialized view {{ this.database }}.{{ this.schema }}.stg_olids_observation_mv cluster by (observation_source_concept_id, clinical_effective_date) as select \"lds_id\" as lds_id, \"id\" as id, \"lds_business_key\" as lds_business_key, \"lds_dataset_id\" as lds_dataset_id, \"lds_datetime_data_acquired\" as lds_datetime_data_acquired, \"lds_initial_data_received_date\" as lds_initial_data_received_date, \"lds_start_date_time\" as lds_start_date_time, \"record_owner_organisation_code\" as record_owner_organisation_code, \"patient_id\" as patient_id, \"person_id\" as person_id, \"encounter_id\" as encounter_id, \"practioner_id\" as practioner_id, \"parent_obervation_id\" as parent_obervation_id, \"clinical_effective_date\" as clinical_effective_date, \"date_precision_concept_id\" as date_precision_concept_id, \"result_value\" as result_value, \"result_value_unit_concept_id\" as result_value_unit_concept_id, \"result_date\" as result_date, \"result_text\" as result_text, \"is_problem\" as is_problem, \"is_review\" as is_review, \"problem_end_date\" as problem_end_date, \"observation_source_concept_id\" as observation_source_concept_id, \"age_at_event\" as age_at_event, \"age_at_event_baby\" as age_at_event_baby, \"age_at_event_neonate\" as age_at_event_neonate, \"episodicity_concept_id\" as episodicity_concept_id, \"is_primary\" as is_primary, \"date_recorded\" as date_recorded, \"is_problem_deleted\" as is_problem_deleted, \"is_confidential\" as is_confidential from {{ source('OLIDS_MASKED', 'OBSERVATION') }}{% elif _rb is sameas false %}create materialized view if not exists {{ this.database }}.{{ this.schema }}.stg_olids_observation_mv cluster by (observation_source_concept_id, clinical_effective_date) as select \"lds_id\" as lds_id, \"id\" as id, \"lds_business_key\" as lds_business_key, \"lds_dataset_id\" as lds_dataset_id, \"lds_datetime_data_acquired\" as lds_datetime_data_acquired, \"lds_initial_data_received_date\" as lds_initial_data_received_date, \"lds_start_date_time\" as lds_start_date_time, \"record_owner_organisation_code\" as record_owner_organisation_code, \"patient_id\" as patient_id, \"person_id\" as person_id, \"encounter_id\" as encounter_id, \"practioner_id\" as practioner_id, \"parent_obervation_id\" as parent_obervation_id, \"clinical_effective_date\" as clinical_effective_date, \"date_precision_concept_id\" as date_precision_concept_id, \"result_value\" as result_value, \"result_value_unit_concept_id\" as result_value_unit_concept_id, \"result_date\" as result_date, \"result_text\" as result_text, \"is_problem\" as is_problem, \"is_review\" as is_review, \"problem_end_date\" as problem_end_date, \"observation_source_concept_id\" as observation_source_concept_id, \"age_at_event\" as age_at_event, \"age_at_event_baby\" as age_at_event_baby, \"age_at_event_neonate\" as age_at_event_neonate, \"episodicity_concept_id\" as episodicity_concept_id, \"is_primary\" as is_primary, \"date_recorded\" as date_recorded, \"is_problem_deleted\" as is_problem_deleted, \"is_confidential\" as is_confidential from {{ source('OLIDS_MASKED', 'OBSERVATION') }}{% endif %}"
  ]
) }}

-- Staging model for OLIDS_MASKED.OBSERVATION
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "patient_id" AS patient_id,
    "person_id" AS person_id,
    "encounter_id" AS encounter_id,
    "practioner_id" AS practioner_id,
    "parent_obervation_id" AS parent_obervation_id,
    "clinical_effective_date" AS clinical_effective_date,
    "date_precision_concept_id" AS date_precision_concept_id,
    "result_value" AS result_value,
    "result_value_unit_concept_id" AS result_value_unit_concept_id,
    "result_date" AS result_date,
    "result_text" AS result_text,
    "is_problem" AS is_problem,
    "is_review" AS is_review,
    "problem_end_date" AS problem_end_date,
    "observation_source_concept_id" AS observation_source_concept_id,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "episodicity_concept_id" AS episodicity_concept_id,
    "is_primary" AS is_primary,
    "date_recorded" AS date_recorded,
    "is_problem_deleted" AS is_problem_deleted,
    "is_confidential" AS is_confidential
FROM {{ source('OLIDS_MASKED', 'OBSERVATION') }}
