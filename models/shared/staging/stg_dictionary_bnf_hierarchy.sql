-- Staging model for dictionary.BNF_Hierarchy
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_BNFID" as sk_bnfid,
    "SK_BNFParentID" as sk_bnfparentid,
    "TypeNum" as typenum,
    "Type" as type,
    "Code" as code,
    "Name" as name,
    "Path" as path,
    "Path_Depth" as path_depth,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'BNF_Hierarchy') }}
