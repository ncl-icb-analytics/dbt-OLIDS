-- Staging model for dictionary.BNF_Chapter
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_BNFChapterID" as sk_bnfchapterid,
    "SK_BNFChapterParentID" as sk_bnfchapterparentid,
    "Chapter_Code" as chapter_code,
    "Chapter_Code_Alt" as chapter_code_alt,
    "Chapter_Code_Alt_Pad" as chapter_code_alt_pad,
    "Chapter_Name" as chapter_name,
    "Chapter_Path" as chapter_path,
    "Chapter_Path_Depth" as chapter_path_depth,
    "IsOfficialBNF" as isofficialbnf,
    "URL" as url,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'BNF_Chapter') }}
