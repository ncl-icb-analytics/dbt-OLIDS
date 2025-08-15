-- Staging model for dictionary.ServiceProviderHierarchy
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "SK_ServiceProviderGroupParentID" as sk_serviceprovidergroupparentid,
    "Level" as level,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'ServiceProviderHierarchy') }}
