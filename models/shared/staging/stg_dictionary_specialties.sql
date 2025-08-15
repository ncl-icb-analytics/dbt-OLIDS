-- Staging model for dictionary.Specialties
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_SpecialtyID" as sk_specialtyid,
    "BK_SpecialtyCode" as bk_specialtycode,
    "SpecialtyName" as specialtyname,
    "SpecialtyCategory" as specialtycategory,
    "IsTreatmentFunction" as istreatmentfunction,
    "IsMainSpecialty" as ismainspecialty,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "MainSpecialtyDescription" as mainspecialtydescription,
    "TreatmentFunctionDescription" as treatmentfunctiondescription
from {{ source('dictionary', 'Specialties') }}
