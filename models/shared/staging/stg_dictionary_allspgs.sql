-- Staging model for dictionary.AllSPGs
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "Level" as level,
    "Type" as type,
    "OriginalID" as originalid,
    "Code" as code,
    "Name" as name,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "IsTestOrganisation" as istestorganisation,
    "IsDormant" as isdormant,
    "IsActive" as isactive
from {{ source('dictionary', 'AllSPGs') }}
