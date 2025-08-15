-- Staging model for dictionary.Staff
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_StaffID" as sk_staffid,
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_OrganisationTypeID" as sk_organisationtypeid,
    "FirstName" as firstname,
    "Surname" as surname,
    "LocalStaffRole" as localstaffrole,
    "StaffCode" as staffcode,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Staff') }}
