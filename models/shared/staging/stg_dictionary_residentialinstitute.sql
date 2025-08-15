-- Staging model for dictionary.ResidentialInstitute
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ResidentialInstituteID" as sk_residentialinstituteid,
    "Cipher" as cipher,
    "ResidentialInstituteCode" as residentialinstitutecode,
    "ResidentialInstituteName" as residentialinstitutename,
    "AttractsGlobalSumUplift" as attractsglobalsumuplift,
    "SK_OrganisationID" as sk_organisationid,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'ResidentialInstitute') }}
