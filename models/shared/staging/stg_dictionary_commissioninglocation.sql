-- Staging model for dictionary.CommissioningLocation
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_CCGLocationId" as sk_ccglocationid,
    "CommissionerCode" as commissionercode,
    "CommissioningRegionCode" as commissioningregioncode,
    "CommissioningRegion" as commissioningregion,
    "LocalAreaTeamCode" as localareateamcode,
    "LocalAreaTeam" as localareateam,
    "CommissioningCounty" as commissioningcounty,
    "CommissioningCountry" as commissioningcountry,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "StartDate" as startdate,
    "EndDate" as enddate
from {{ source('dictionary', 'CommissioningLocation') }}
