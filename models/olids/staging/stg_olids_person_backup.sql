-- Staging model for olids_core.PERSON_BACKUP
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "lds_end_date_time" as lds_end_date_time,
    "requesting_patient_record_id" as requesting_patient_record_id,
    "unique_reference" as unique_reference,
    "requesting_nhs_numberhash" as requesting_nhs_numberhash,
    "errror_success_code" as errror_success_code,
    "matched_nhs_numberhash" as matched_nhs_numberhash,
    "sensitivity_flag" as sensitivity_flag,
    "matched_algorithm_indicator" as matched_algorithm_indicator,
    "requesting_patient_id" as requesting_patient_id
from {{ source('olids_core', 'PERSON_BACKUP') }}
