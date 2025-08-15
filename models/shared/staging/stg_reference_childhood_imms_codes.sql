-- Staging model for reference.CHILDHOOD_IMMS_CODES
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "VACCINE" as vaccine,
    "DOSE" as dose,
    "PROPOSEDCLUSTER" as proposedcluster,
    "SOURCECLUSTERID" as sourceclusterid,
    "SOURCECLUSTERDESCRIPTION" as sourceclusterdescription,
    "SNOMEDCONCEPTID" as snomedconceptid,
    "CODEDESCRIPTION" as codedescription,
    "SOURCE" as source
from {{ source('reference', 'CHILDHOOD_IMMS_CODES') }}
