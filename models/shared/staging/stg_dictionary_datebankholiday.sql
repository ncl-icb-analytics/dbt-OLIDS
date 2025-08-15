-- Staging model for dictionary.DateBankHoliday
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_Date" as sk_date,
    "FullDate" as fulldate,
    "Holiday" as holiday,
    "InEnglandAndWales" as inenglandandwales,
    "InNorthernIreland" as innorthernireland,
    "InScotland" as inscotland
from {{ source('dictionary', 'DateBankHoliday') }}
