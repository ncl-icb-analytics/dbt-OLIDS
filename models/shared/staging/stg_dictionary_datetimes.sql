-- Staging model for dictionary.DateTimes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_DateTime" as sk_datetime,
    "SK_Date" as sk_date,
    "SK_Time" as sk_time,
    "FullDateTime" as fulldatetime,
    "FullDate" as fulldate,
    "FullTime" as fulltime
from {{ source('dictionary', 'DateTimes') }}
