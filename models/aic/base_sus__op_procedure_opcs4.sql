{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    opcs_id,
    "code" as code,
    "date" as date,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "main_operating_professional.registration_issuer"
    as main_operating_professional_registration_issuer,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "dmicImportLogId" as dmic_import_log_id
from {{ source("sus_op", "appointment_clinical_coding_procedure_opcs") }}
