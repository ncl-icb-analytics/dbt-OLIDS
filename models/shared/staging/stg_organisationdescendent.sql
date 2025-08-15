-- Staging model for "dbo".OrganisationDescendent
-- Source: "Dictionary"."dbo"

select
    "OrganisationCode_Child" as organisationcode_child,
    "OrganisationPrimaryRole_Child" as organisationprimaryrole_child,
    "Path" as path,
    "RelationshipStartDate" as relationshipstartdate
from {{ source('dictionary', 'OrganisationDescendent') }}
