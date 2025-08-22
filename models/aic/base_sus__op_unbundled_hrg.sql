{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    unbundled_hrg_id,
    "code" as code,
    "multiple_applies" as multiple_applies,
    "tariff" as tariff,
    "dmicImportLogId" as dmicimportlogid
from {{ source("sus_op", "appointment_commissioning_grouping_unbundled_hrg") }}
