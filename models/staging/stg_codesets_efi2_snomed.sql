-- Staging model for CODESETS.EFI2_SNOMED
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "DEFICIT" AS deficit,
    "SNOMEDCT_CONCEPTID" AS snomedct_conceptid,
    "CTV3" AS ctv3,
    "PROVENANCE" AS provenance,
    "CODEDESCRIPTION" AS codedescription,
    "TIMECONSTRAINTYEARS" AS timeconstraintyears,
    "AGELIMIT" AS agelimit,
    "OTHERINSTRUCTIONS" AS otherinstructions
FROM {{ source('REFERENCE', 'EFI2_SNOMED') }}
