-- Staging model for dictionary.Dates_Month_Year
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_YearMonth" as sk_yearmonth,
    "MonthNum" as monthnum,
    "MonthName" as monthname,
    "CalendarMonthNumber" as calendarmonthnumber,
    "CalendarYearNumber" as calendaryearnumber,
    "StartOfMonthDate" as startofmonthdate,
    "EndOfMonthDate" as endofmonthdate,
    "SK_Date_StartOfMonth" as sk_date_startofmonth,
    "SK_Date_EndOfMonth" as sk_date_endofmonth
from {{ source('dictionary', 'Dates_Month_Year') }}
