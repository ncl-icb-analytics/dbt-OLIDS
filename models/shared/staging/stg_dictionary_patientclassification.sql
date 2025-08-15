-- Staging model for dictionary.PatientClassification
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_PatientClassificationID" as sk_patientclassificationid,
    "BK_PatientClassificationCode" as bk_patientclassificationcode,
    "PatientClassificationName" as patientclassificationname,
    "PatientClassificationFullName" as patientclassificationfullname
from {{ source('dictionary', 'PatientClassification') }}
