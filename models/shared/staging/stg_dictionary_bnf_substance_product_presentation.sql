-- Staging model for dictionary.BNF_Substance_Product_Presentation
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_BNFID" as sk_bnfid,
    "SK_BNFParentID" as sk_bnfparentid,
    "SK_BNFChapterID" as sk_bnfchapterid,
    "TypeNum" as typenum,
    "Type" as type,
    "Code" as code,
    "Name" as name,
    "Path" as path,
    "Path_Depth" as path_depth,
    "IsSubstance" as issubstance,
    "IsProduct" as isproduct,
    "IsPresentation" as ispresentation,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "IsGeneric" as isgeneric,
    "SK_BNFID_GenericEquivalent" as sk_bnfid_genericequivalent
from {{ source('dictionary', 'BNF_Substance_Product_Presentation') }}
