-- Staging model for dictionary.ONSAreaCollections
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_AreaCollectionID" as sk_areacollectionid,
    "AreaCode" as areacode,
    "AreaName" as areaname,
    "Country" as country
from {{ source('dictionary', 'ONSAreaCollections') }}
