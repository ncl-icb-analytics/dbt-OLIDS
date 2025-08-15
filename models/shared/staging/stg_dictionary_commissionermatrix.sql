-- Staging model for dictionary.CommissionerMatrix
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_CommissionerID" as sk_commissionerid,
    "SK_CommissionerGroupID" as sk_commissionergroupid,
    "SK_CommissionerOrgID" as sk_commissionerorgid,
    "SK_PODTeamID" as sk_podteamid,
    "SK_PODTeamGroupID" as sk_podteamgroupid,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stpgroupid,
    "SK_PCTID" as sk_pctid,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "NumberOfPractices" as numberofpractices
from {{ source('dictionary', 'CommissionerMatrix') }}
