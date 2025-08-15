-- Staging model for dictionary.Unit
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_UnitID" as sk_unitid,
    "UnitSymbol" as unitsymbol,
    "UnitName" as unitname,
    "QuantityName" as quantityname,
    "SIPower" as sipower,
    "IsStandardUnit" as isstandardunit,
    "IsDerivedUnit" as isderivedunit
from {{ source('dictionary', 'Unit') }}
