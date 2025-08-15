-- Staging model for dictionary.PracticeMatrixNational
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "SK_ServiceProviderOrgID" as sk_serviceproviderorgid,
    "SK_NetworkID" as sk_networkid,
    "SK_CommissionerID" as sk_commissionerid,
    "SK_CommissionerGroupID" as sk_commissionergroupid,
    "SK_CommissionerOrgID" as sk_commissionerorgid,
    "SK_PODTeamID" as sk_podteamid,
    "SK_PODTeamGroupID" as sk_podteamgroupid,
    "SK_STPID" as sk_stpid,
    "SK_STPGroupID" as sk_stpgroupid,
    "SK_PCTID" as sk_pctid,
    "SK_CCGLocationID" as sk_ccglocationid,
    "IsPracticeActive" as ispracticeactive,
    "IsPracticeDormant" as ispracticedormant,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'PracticeMatrixNational') }}
