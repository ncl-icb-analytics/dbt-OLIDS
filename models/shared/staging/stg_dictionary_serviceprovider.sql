-- Staging model for dictionary.ServiceProvider
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "ServiceProviderCode" as serviceprovidercode,
    "ServiceProviderName" as serviceprovidername,
    "ServiceProviderType" as serviceprovidertype,
    "SK_PostcodeID" as sk_postcodeid,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "IsActive" as isactive,
    "IsMainSite" as ismainsite,
    "IsTestOrganisation" as istestorganisation,
    "IsDormant" as isdormant,
    "ServiceProviderFullCode" as serviceproviderfullcode
from {{ source('dictionary', 'ServiceProvider') }}
