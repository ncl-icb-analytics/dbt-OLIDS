-- Staging model for dictionary.Diagnosis
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_DiagnosisID" as sk_diagnosisid,
    "Code" as code,
    "AltCode" as altcode,
    "Description" as description,
    "ShortDescription" as shortdescription,
    "Modifiers" as modifiers,
    "Chapter_Number" as chapter_number,
    "Chapter" as chapter,
    "SubChapter" as subchapter,
    "Gender_Mask" as gender_mask,
    "Min_Age" as min_age,
    "Max_Age" as max_age,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SubChapterCode" as subchaptercode,
    "SubChapter2" as subchapter2,
    "SubChapter2Code" as subchapter2code,
    "SubChapter3" as subchapter3,
    "SubChapter3Code" as subchapter3code
from {{ source('dictionary', 'Diagnosis') }}
