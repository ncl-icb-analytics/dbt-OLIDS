{{ config(materialized='view') }}

select
    rownumber_id,
    primarykey_id,
    snomed_id,
    "is_aec_related" as is_aec_related,
    "is_allergy_related" as is_allergy_related,
    "is_applicable_to_females" as is_applicable_to_females,
    "is_applicable_to_males" as is_applicable_to_males,
    "is_notifiable_disease" as is_notifiable_disease,
    "is_code_approved" as is_code_approved,
    "is_injury_related" as is_injury_related,
    "equivalent_ae_code" as equivalent_ae_code,
    "is_primary" as is_primary,
    "qualifier" as qualifier,
    "is_qualifier_approved" as is_qualifier_approved,
    "sequence_number" as sequence_number,
    "code" as code,
    "dmicImportLogId" as dmicimportlogid
from {{ source("sus_ecds", "clinical_diagnoses_snomed") }}
