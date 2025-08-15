-- Staging model for dictionary.HRG
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_HRGID" as sk_hrgid,
    "HRGCode" as hrgcode,
    "HRGDescription" as hrgdescription,
    "HRGChapterKey" as hrgchapterkey,
    "HRGChapter" as hrgchapter,
    "HRGSubchapterKey" as hrgsubchapterkey,
    "HRGSubchapter" as hrgsubchapter,
    "HRG_Version" as hrg_version,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'HRG') }}
