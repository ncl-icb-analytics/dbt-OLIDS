-- Staging model for olids_core.LOCATION
-- Base layer: base_olids_location (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "name" as name,
    "type_code" as type_code,
    "type_desc" as type_desc,
    "is_primary_location" as is_primary_location,
    "house_name" as house_name,
    "house_number" as house_number,
    "house_name_flat_number" as house_name_flat_number,
    "street" as street,
    "address_line_1" as address_line_1,
    "address_line_2" as address_line_2,
    "address_line_3" as address_line_3,
    "address_line_4" as address_line_4,
    "postcode" as postcode,
    "managing_organisation_id" as managing_organisation_id,
    "open_date" as open_date,
    "close_date" as close_date,
    "is_obsolete" as is_obsolete
from {{ ref('base_olids_location') }}
