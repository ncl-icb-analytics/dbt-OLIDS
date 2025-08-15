-- Staging model for reference.EFI2_SNOMED
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "DEFICIT" as deficit,
    "SNOMEDCT_CONCEPTID" as snomedct_conceptid,
    "CTV3" as ctv3,
    "PROVENANCE" as provenance,
    "CODEDESCRIPTION" as codedescription,
    "TIMECONSTRAINTYEARS" as timeconstraintyears,
    "AGELIMIT" as agelimit,
    "OTHERINSTRUCTIONS" as otherinstructions
from {{ source('reference', 'EFI2_SNOMED') }}
