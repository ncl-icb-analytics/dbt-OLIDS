-- Staging model for dictionary.Gender
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_GenderID" as sk_genderid,
    "Gender" as gender,
    "GenderCode" as gendercode,
    "GenderCode1" as gendercode1,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "GenderCode2" as gendercode2
from {{ source('dictionary', 'Gender') }}
