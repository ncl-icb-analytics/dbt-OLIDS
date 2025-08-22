{{ config(materialized='view') }}

select
    primarykey_id,
    episodes_id,
    icd_id,
    rownumber_id,
    "present_on_admission" as present_on_admission,
    "code" as code,
    "dmicImportLogId" as dmic_import_log_id
from {{ source("sus_apc", "spell_episodes_diagnosis_icd") }}
