-- Staging model for dictionary.ServiceProviderGroup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "ServiceProviderGroupName" as serviceprovidergroupname,
    "ServiceProviderGroupCode" as serviceprovidergroupcode,
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "IsTestOrganisation" as istestorganisation
from {{ source('dictionary', 'ServiceProviderGroup') }}
