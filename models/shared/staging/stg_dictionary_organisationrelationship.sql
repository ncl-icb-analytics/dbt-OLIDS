-- Staging model for dictionary.OrganisationRelationship
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID" as sk_organisationid,
    "SK_OrganisationID_Target" as sk_organisationid_target,
    "TargetRelationshipType" as targetrelationshiptype,
    "TargetPrimaryRoleType" as targetprimaryroletype,
    "LegalStartDate" as legalstartdate,
    "LegalEndDate" as legalenddate,
    "OperationalStartDate" as operationalstartdate,
    "OperationalEndDate" as operationalenddate,
    "TargetAssignedBy" as targetassignedby,
    "IsActive" as isactive,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "UniqueRelationshipID" as uniquerelationshipid
from {{ source('dictionary', 'OrganisationRelationship') }}
