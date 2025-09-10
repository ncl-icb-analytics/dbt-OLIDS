-- Staging model for olids_core.ORGANISATION
-- Source: "Data_Store_OLIDS_Alpha"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_dataset_id" as lds_dataset_id,
    "lds_start_date_time" as lds_start_date_time,
    "organisation_code" as organisation_code,
    "assigning_authority_code" as assigning_authority_code,
    "name" as name,
    "type_code" as type_code,
    "type_desc" as type_desc,
    "postcode" as postcode,
    "parent_organisation_id" as parent_organisation_id,
    "open_date" as open_date,
    "close_date" as close_date,
    "is_obsolete" as is_obsolete
from {{ source('olids_core', 'ORGANISATION') }}
