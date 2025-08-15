-- Staging model for dictionary.CCGByPractice
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "SK_OrganisationID_ServiceProvider" as sk_organisationid_serviceprovider,
    "ServiceProviderCode" as serviceprovidercode,
    "ServiceProviderName" as serviceprovidername,
    "ProviderGroupID" as providergroupid,
    "ProviderGroup" as providergroup,
    "CommissionerID" as commissionerid,
    "CommissionerGroupID" as commissionergroupid,
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "CommissionerCode" as commissionercode,
    "Commissioner" as commissioner,
    "PODID" as podid,
    "PODGroupID" as podgroupid,
    "PODCode" as podcode,
    "POD" as pod,
    "PODName" as podname,
    "STPID" as stpid,
    "STPGroupID" as stpgroupid,
    "STPCode" as stpcode,
    "STP" as stp,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "CommissioningRegionCode" as commissioningregioncode,
    "CommissioningRegion" as commissioningregion,
    "LocalAreaTeamCode" as localareateamcode,
    "LocalAreaTeam" as localareateam,
    "CommissioningCounty" as commissioningcounty,
    "CommissioningCountry" as commissioningcountry,
    "SK_Organisation_ID_ServiceProvider" as sk_organisation_id_serviceprovider,
    "SK_Organisation_ID_Commissioner" as sk_organisation_id_commissioner
from {{ source('dictionary', 'CCGByPractice') }}
