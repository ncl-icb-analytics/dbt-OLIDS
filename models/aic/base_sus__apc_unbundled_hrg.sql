{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    episodes_id,
    unbundled_hrg_id,
    "code" as code,
    "adult_cc_tariff_days" as adult_cc_tariff_days,
    "multiple_applies" as multiple_applies,
    "tariff" as tariff,
    "dmicImportLogId" as dmicimportlogid
from {{ source("sus_apc", "spell_episodes_commissioning_grouping_unbundled_hrg") }}
