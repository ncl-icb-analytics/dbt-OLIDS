{{ config(materialized='view') }}

select
    "SK_SnomedConceptID" as sk_snomed_concept_id,
    "SK_SnomedDescriptionID" as sk_snomed_description_id,
    "SK_SnomedModuleID" as sk_snomed_module_id,
    "PreferredTerm" as preferred_term,
    "SK_SnomedDefinitionStatusID" as sk_snomed_definition_status_id,
    "DefinitionStatus" as definition_status,
    "IsActive" as is_active,
    "InNationalDataset" as in_national_dataset,
    "LastUpdated" as last_updated
from {{ source('snomed', 'Concept') }}