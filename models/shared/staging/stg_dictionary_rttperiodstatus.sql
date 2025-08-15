-- Staging model for dictionary.RTTPeriodStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_RTTPeriodStatusID" as sk_rttperiodstatusid,
    "BK_RTTPeriodStatusCode" as bk_rttperiodstatuscode,
    "RTTPeriodStatusCategory" as rttperiodstatuscategory,
    "RTTPeriodStatusDescription" as rttperiodstatusdescription,
    "Notes" as notes
from {{ source('dictionary', 'RTTPeriodStatus') }}
