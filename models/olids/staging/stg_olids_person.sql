-- Staging model for olids_core.PERSON
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data
--
-- INTERIM FIX: Generate reliable person_id (id field) from sk_patient_id_matched due to duplicate person_id in source
-- See issue #192: https://github.com/ncl-icb-analytics/dbt-OLIDS/issues/192

{{
    config(
        materialized='table'
    )
}}

select distinct
    "lds_id" as lds_id,
    -- Generate deterministic id from sk_patient_id_matched hash to match patient_person bridge
    'person-' || MD5("sk_patient_id_matched") as id,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "lds_end_date_time" as lds_end_date_time,
    "requesting_patient_record_id" as requesting_patient_record_id,
    "unique_reference" as unique_reference,
    "requesting_nhs_number_hash" as requesting_nhs_number_hash,
    "sk_patient_id_request" as sk_patient_id_request,
    "error_success_code" as error_success_code,
    "matched_nhs_numberhash" as matched_nhs_numberhash,
    "sk_patient_id_matched" as sk_patient_id_matched,
    "sensitivity_flag" as sensitivity_flag,
    "matched_algorithm_indicator" as matched_algorithm_indicator,
    "requesting_patient_id" as requesting_patient_id,
    -- Keep original id for rollback when source is fixed
    "id" as original_id
from {{ source('olids_core', 'PERSON') }}
where "sk_patient_id_matched" is not null
