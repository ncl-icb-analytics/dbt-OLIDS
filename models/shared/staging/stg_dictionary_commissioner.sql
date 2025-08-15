-- Staging model for dictionary.Commissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_CommissionerID" as sk_commissionerid,
    "CommissionerName" as commissionername,
    "CommissionerType" as commissionertype,
    "CommissionerCode" as commissionercode,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "IsCustomer" as iscustomer,
    "IsTestOrganisation" as istestorganisation
from {{ source('dictionary', 'Commissioner') }}
