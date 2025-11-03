/*
POSTCODE_HASH Base View
OLIDS reference data linking postcode hashes to geographical areas.
Passthrough view with standard column naming applied.
*/

SELECT
    id,
    postcode_hash,
    primary_care_organisation,
    local_authority_organisation,
    yr2011_lsoa,
    yr2011_msoa,
    yr2021_lsoa,
    yr2021_msoa,
    effective_from,
    effective_to,
    is_latest,
    lds_is_deleted,
    lds_start_date_time,
    lakehousedateprocessed,
    high_watermark_date_time
FROM {{ source('olids_reference', 'POSTCODE_HASH') }}
