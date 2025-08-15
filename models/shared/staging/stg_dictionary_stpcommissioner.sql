-- Staging model for dictionary.STPCommissioner
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_STPID" as sk_stpid,
    "SK_CommissionerID" as sk_commissionerid,
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "SK_OrganisationID_STP" as sk_organisationid_stp
from {{ source('dictionary', 'STPCommissioner') }}
