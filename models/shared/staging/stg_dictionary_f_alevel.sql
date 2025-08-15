-- Staging model for dictionary.F&ALevel
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_LevelID" as sk_levelid,
    "LevelName" as levelname
from {{ source('dictionary', 'F&ALevel') }}
