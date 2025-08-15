-- Staging model for dictionary.OrganisationType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationTypeID" as sk_organisationtypeid,
    "OrganisationType" as organisationtype,
    "ShortOrganisationType" as shortorganisationtype,
    "CodeAllocatedBy" as codeallocatedby,
    "IsOrganisationCode" as isorganisationcode,
    "IsLocationCode" as islocationcode,
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid
from {{ source('dictionary', 'OrganisationType') }}
