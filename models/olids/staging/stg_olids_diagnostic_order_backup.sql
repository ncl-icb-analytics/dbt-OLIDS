-- Staging model for olids_core.DIAGNOSTIC_ORDER_BACKUP
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "patient_id" as patient_id,
    "encounter_id" as encounter_id,
    "practitioner_id" as practitioner_id,
    "parent_observation_id" as parent_observation_id,
    "clinical_effective_date" as clinical_effective_date,
    "date_precision_concept_id" as date_precision_concept_id,
    "result_value" as result_value,
    "result_value_units" as result_value_units,
    "result_date" as result_date,
    "result_text" as result_text,
    "is_problem" as is_problem,
    "is_review" as is_review,
    "problem_end_date" as problem_end_date,
    "diagnostic_order_core_concept_id" as diagnostic_order_core_concept_id,
    "diagnostic_order_raw_concept_id" as diagnostic_order_raw_concept_id,
    "age_at_event" as age_at_event,
    "age_at_event_baby" as age_at_event_baby,
    "age_at_event_neonate" as age_at_event_neonate,
    "episodicity_concept_id" as episodicity_concept_id,
    "is_primary" as is_primary,
    "date_recorded" as date_recorded,
    "person_id" as person_id
from {{ source('olids_core', 'DIAGNOSTIC_ORDER_BACKUP') }}
