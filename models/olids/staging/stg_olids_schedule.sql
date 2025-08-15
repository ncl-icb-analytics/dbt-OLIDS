-- Staging model for olids_core.SCHEDULE
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "location_id" as location_id,
    "location" as location,
    "practitioner_id" as practitioner_id,
    "start_date" as start_date,
    "end_date" as end_date,
    "type" as type,
    "name" as name,
    "is_private" as is_private
from {{ source('olids_core', 'SCHEDULE') }}
