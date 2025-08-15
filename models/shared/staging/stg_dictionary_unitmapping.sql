-- Staging model for dictionary.UnitMapping
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_UnitID" as sk_unitid,
    "UnitLabel" as unitlabel
from {{ source('dictionary', 'UnitMapping') }}
