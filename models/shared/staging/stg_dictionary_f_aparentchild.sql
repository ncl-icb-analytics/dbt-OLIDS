-- Staging model for dictionary.F&AParentChild
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_PARENT_PODID" as sk_parent_podid,
    "SK_CHILD_PODID" as sk_child_podid
from {{ source('dictionary', 'F&AParentChild') }}
