-- Staging model for dictionary.OrganisationRole
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID" as sk_organisationid,
    "RoleType" as roletype,
    "LegalStartDate" as legalstartdate,
    "LegalEndDate" as legalenddate,
    "OperationalStartDate" as operationalstartdate,
    "OperationalEndDate" as operationalenddate,
    "IsPrimaryRole" as isprimaryrole,
    "IsActive" as isactive,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "UniqueRoleID" as uniqueroleid
from {{ source('dictionary', 'OrganisationRole') }}
