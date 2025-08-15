-- Staging model for dictionary.OrganisationMatrixCommissionerView
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "CommissionerCode" as commissionercode,
    "CommissionerName" as commissionername,
    "SK_OrganisationID_STP" as sk_organisationid_stp,
    "STPCode" as stpcode,
    "STPName" as stpname
from {{ source('dictionary', 'OrganisationMatrixCommissionerView') }}
