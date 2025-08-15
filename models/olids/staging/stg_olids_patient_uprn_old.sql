-- Staging model for olids_core.PATIENT_UPRN_OLD
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "registrar_event_id" as registrar_event_id,
    "masked_uprn" as masked_uprn,
    "masked_upsn" as masked_upsn,
    "masked_postcode" as masked_postcode,
    "address_format_quality" as address_format_quality,
    "post_code_quality" as post_code_quality,
    "matched_with_assign" as matched_with_assign,
    "qualifier" as qualifier,
    "uprn_property_classification" as uprn_property_classification,
    "algorithm" as algorithm,
    "match_pattern" as match_pattern,
    "lds_end_date_time" as lds_end_date_time
from {{ source('olids_core', 'PATIENT_UPRN_OLD') }}
