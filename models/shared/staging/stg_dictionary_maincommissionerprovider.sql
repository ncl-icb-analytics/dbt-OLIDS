-- Staging model for dictionary.MainCommissionerProvider
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "SK_CommissionerID" as sk_commissionerid,
    "SK_OrganisationID_Provider" as sk_organisationid_provider,
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "StartDate" as startdate,
    "EndDate" as enddate
from {{ source('dictionary', 'MainCommissionerProvider') }}
