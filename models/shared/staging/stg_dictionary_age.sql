-- Staging model for dictionary.Age
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_AgeID" as sk_ageid,
    "Age" as age,
    "SK_AgeBandID" as sk_agebandid,
    "SK_AgeBandGPID" as sk_agebandgpid
from {{ source('dictionary', 'Age') }}
