{{ config(materialized='view') }}

select
    "date" as date,
    "OPCS_ID" as opcs_id,
    "code" as code,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "main_operating_professional.registration_issuer"
    as main_operating_professional_registration_issuer,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "dmicImportLogId" as dmic_import_log_id,
    "EPISODES_ID" as episodes_id,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
from {{ source("sus_apc", "spell_episodes_procedure_opcs") }}
