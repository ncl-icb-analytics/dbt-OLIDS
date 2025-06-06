-- Staging model for CODESETS.EFI2_SNOMED
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "DEFICIT" as deficit,
    "SNOMEDCT_CONCEPTID" as snomedct_conceptid,
    "CTV3" as ctv3,
    "PROVENANCE" as provenance,
    "CODEDESCRIPTION" as codedescription,
    "TIMECONSTRAINTYEARS" as timeconstraintyears,
    "AGELIMIT" as agelimit,
    "OTHERINSTRUCTIONS" as otherinstructions
from {{ source('CODESETS', 'EFI2_SNOMED') }}
