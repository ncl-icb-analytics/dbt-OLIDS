-- Staging model for dictionary.OrganisationHierarchyPractice
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID" as sk_organisationid,
    "SK_OrganisationID_Parent" as sk_organisationid_parent,
    "Level" as level
from {{ source('dictionary', 'OrganisationHierarchyPractice') }}
