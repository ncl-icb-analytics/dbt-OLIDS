-- Staging model for reference.PRACTICE_NEIGHBOURHOOD_LOOKUP
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "PRACTICECODE" as practicecode,
    "PRACTICENAME" as practicename,
    "PCNCODE" as pcncode,
    "LOCALAUTHORITY" as localauthority,
    "PRACTICENEIGHBOURHOOD" as practiceneighbourhood
from {{ source('reference', 'PRACTICE_NEIGHBOURHOOD_LOOKUP') }}
