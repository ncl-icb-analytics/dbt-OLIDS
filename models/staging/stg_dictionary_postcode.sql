-- Staging model for Dictionary.dbo.Postcode
-- Source: Dictionary.dbo

SELECT
    "SK_PostcodeID" AS sk_postcode_id,
    "Postcode_single_space_e_Gif" AS postcode,
    "LSOA" AS lsoa,
    "MSOA" AS msoa,
    "Latitude" AS latitude,
    "Longitude" AS longitude
FROM {{ source('Dictionary_dbo', 'Postcode') }}