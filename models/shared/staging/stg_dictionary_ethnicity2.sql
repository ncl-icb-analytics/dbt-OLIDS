-- Staging model for dictionary.Ethnicity2
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_EthnicityID" as sk_ethnicityid,
    "EthnicityCategory" as ethnicitycategory,
    "EthnicityDesc" as ethnicitydesc
from {{ source('dictionary', 'Ethnicity2') }}
