-- Staging model for dictionary.OrganisationMatrixPractice
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID_Practice" as sk_organisationid_practice,
    "SK_OrganisationID_Network" as sk_organisationid_network,
    "SK_OrganisationID_Commissioner" as sk_organisationid_commissioner,
    "SK_OrganisationID_STP" as sk_organisationid_stp
from {{ source('dictionary', 'OrganisationMatrixPractice') }}
