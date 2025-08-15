-- Staging model for dictionary.PODGroups
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_PodGroupID" as sk_podgroupid,
    "PodDisplay" as poddisplay,
    "PodDataset" as poddataset,
    "PodMainGroup" as podmaingroup,
    "PodSubGroup" as podsubgroup,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'PODGroups') }}
