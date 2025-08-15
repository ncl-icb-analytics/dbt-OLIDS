-- Staging model for dictionary.Language
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_LanguageID" as sk_languageid,
    "LanguageSpoken" as languagespoken,
    "CDSCode" as cdscode,
    "Read2Code" as read2code,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Language') }}
