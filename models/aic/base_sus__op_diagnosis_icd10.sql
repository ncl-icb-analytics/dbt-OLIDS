{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    icd_id,
    "code" as code,
    "present_on_admission" as present_on_admission,
    "dmicImportLogId" as dmic_import_log_id
from {{ source("sus_op", "appointment_clinical_coding_diagnosis_icd") }}
