{{
    config(
        secure=true,
        alias='ndoo_hashed')
}}

/*
NDOO_HASHED Base View
National Data Opt-Out preferences keyed by hashed NHS number.
Passthrough from NDOO_MASKED.PATIENT_HASH; surfaced downstream as ndoo_hashed.
*/

SELECT
    id,
    lds_record_id,
    sk_patient_id,
    nhs_number_hash,
    preference_type,
    preference_status,
    lds_is_deleted,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_batch_id,
    lds_file_id,
    lds_dataset_id,
    effective_from,
    effective_to,
    is_latest,
    lakehouse_date_processed,
    high_watermark_date_time
FROM {{ source('ndoo_masked', 'PATIENT_HASH') }}
