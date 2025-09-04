-- Staging model for reference.IMD2019
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "LSOACODE" as lsoacode,
    "IMDDECILE" as imddecile
from {{ source('reference', 'IMD2019') }}
