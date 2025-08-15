-- Staging model for "dbo".Postcode
-- Source: "Dictionary"."dbo"

select
    "SK_PostcodeID" as sk_postcodeid,
    "Postcode_single_space_e_Gif" as postcode_single_space_e_gif,
    "LSOA" as lsoa,
    "MSOA" as msoa,
    "Latitude" as latitude,
    "Longitude" as longitude
from {{ source('dictionary', 'Postcode') }}
