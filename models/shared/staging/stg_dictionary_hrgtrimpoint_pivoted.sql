-- Staging model for dictionary.HRGTrimPoint_Pivoted
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_HRGID" as sk_hrgid,
    "TariffType" as tarifftype,
    "TariffTypeDesc" as tarifftypedesc,
    "FiscalYear" as fiscalyear,
    "Elective_TrimPointDays" as elective_trimpointdays,
    "Non-Elective_TrimPointDays" as non_elective_trimpointdays
from {{ source('dictionary', 'HRGTrimPoint_Pivoted') }}
