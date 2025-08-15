-- Staging model for dictionary.Procedure
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ProcedureCode" as sk_procedurecode,
    "Code" as code,
    "Alt_Code" as alt_code,
    "Category" as category,
    "Description" as description,
    "Status_Of_Operation" as status_of_operation,
    "Sex_Absolute" as sex_absolute,
    "Sex_Scrutiny" as sex_scrutiny,
    "Method_Of_Delivery_Code" as method_of_delivery_code,
    "OPCS_Version" as opcs_version,
    "IsOnlySecondaryCode" as isonlysecondarycode,
    "IsOnlyFemales" as isonlyfemales,
    "IsOnlyMales" as isonlymales,
    "IsMainlyFemales" as ismainlyfemales,
    "IsMainlyMales" as ismainlymales,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "Chapter" as chapter
from {{ source('dictionary', 'Procedure') }}
