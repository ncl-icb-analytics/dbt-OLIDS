-- Staging model for dictionary.OrganisationONSCode
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID" as sk_organisationid,
    "ONS_Code" as ons_code
from {{ source('dictionary', 'OrganisationONSCode') }}
