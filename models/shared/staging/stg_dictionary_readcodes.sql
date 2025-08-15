-- Staging model for dictionary.ReadCodes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ReadCodeID" as sk_readcodeid,
    "ReadCode" as readcode,
    "Term" as term,
    "SnomedCTCode" as snomedctcode,
    "InNationalDataset" as innationaldataset,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "IsSensitive" as issensitive,
    "SK_ReadCodeParentID" as sk_readcodeparentid,
    "SK_BNFChapterID" as sk_bnfchapterid,
    "IsReadV2" as isreadv2,
    "IsCTV3" as isctv3,
    "ReadCodeAlt" as readcodealt,
    "SK_ReadCodeParentID_ReadV2" as sk_readcodeparentid_readv2,
    "SK_ReadCodeParentID_CTV3" as sk_readcodeparentid_ctv3,
    "MatchesReadCodeRegex" as matchesreadcoderegex
from {{ source('dictionary', 'ReadCodes') }}
