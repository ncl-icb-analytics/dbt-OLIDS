-- Staging model for dictionary.OrganisationSuccessor
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OrganisationID" as sk_organisationid,
    "SK_OrganisationID_Successor" as sk_organisationid_successor,
    "Succession_Effective_Date" as succession_effective_date,
    "SK_OrganisationID_FinalSuccessor" as sk_organisationid_finalsuccessor,
    "UniqueKey" as uniquekey,
    "SuccessorType" as successortype,
    "SuccessorPrimaryRoleType" as successorprimaryroletype,
    "SuccessorAssignedBy" as successorassignedby,
    "UniqueSuccessorID" as uniquesuccessorid
from {{ source('dictionary', 'OrganisationSuccessor') }}
