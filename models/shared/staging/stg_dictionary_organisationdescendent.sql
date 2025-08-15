-- Staging model for dictionary.OrganisationDescendent
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID_Root" as sk_organisationid_root,
    "OrganisationCode_Root" as organisationcode_root,
    "OrganisationPrimaryRole_Root" as organisationprimaryrole_root,
    "SK_OrganisationID_Parent" as sk_organisationid_parent,
    "OrganisationCode_Parent" as organisationcode_parent,
    "OrganisationPrimaryRole_Parent" as organisationprimaryrole_parent,
    "SK_OrganisationID_Child" as sk_organisationid_child,
    "OrganisationCode_Child" as organisationcode_child,
    "OrganisationPrimaryRole_Child" as organisationprimaryrole_child,
    "RelationshipType" as relationshiptype,
    "RelationshipStartDate" as relationshipstartdate,
    "RelationshipEndDate" as relationshipenddate,
    "Path" as path,
    "Depth" as depth,
    "PathStartDate" as pathstartdate,
    "PathEndDate" as pathenddate,
    "DateAdded" as dateadded,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'OrganisationDescendent') }}
