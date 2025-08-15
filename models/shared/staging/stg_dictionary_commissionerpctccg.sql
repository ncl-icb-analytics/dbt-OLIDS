-- Staging model for dictionary.CommissionerPCTCCG
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_CommissionerID_PCT" as sk_commissionerid_pct,
    "SK_CommissionerID_CCG" as sk_commissionerid_ccg,
    "SK_PODTeamID" as sk_podteamid,
    "PODName" as podname,
    "Region" as region,
    "StartDate" as startdate,
    "EndDate" as enddate
from {{ source('dictionary', 'CommissionerPCTCCG') }}
