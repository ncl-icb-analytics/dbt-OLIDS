-- Staging model for dictionary.STPByCommissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_CommissionerID" as sk_commissionerid,
    "SK_CommissionerGroupID" as sk_commissionergroupid,
    "SK_CommissionerOrgID" as sk_commissionerorgid,
    "CommissionerCode" as commissionercode,
    "CommissionerName" as commissionername,
    "SK_PODTeamID" as sk_podteamid,
    "SK_PODTeamGroupID" as sk_podteamgroupid,
    "PODTeamCode" as podteamcode,
    "PODTeamName" as podteamname,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stpgroupid,
    "STPCode" as stpcode,
    "STPName" as stpname,
    "SK_PCTID" as sk_pctid,
    "NumberOfPractices" as numberofpractices,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'STPByCommissioner') }}
