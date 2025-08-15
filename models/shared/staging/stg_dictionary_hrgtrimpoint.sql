-- Staging model for dictionary.HRGTrimPoint
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_HRGID" as sk_hrgid,
    "SK_TariffTypeID" as sk_tarifftypeid,
    "FiscalYear" as fiscalyear,
    "IsElectiveStay" as iselectivestay,
    "TrimPointDays" as trimpointdays
from {{ source('dictionary', 'HRGTrimPoint') }}
