-- Staging model for dictionary.OrganisationRelationshipType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "Code" as code,
    "Name" as name
from {{ source('dictionary', 'OrganisationRelationshipType') }}
