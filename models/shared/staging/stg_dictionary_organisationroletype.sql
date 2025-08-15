-- Staging model for dictionary.OrganisationRoleType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "Code" as code,
    "Name" as name
from {{ source('dictionary', 'OrganisationRoleType') }}
