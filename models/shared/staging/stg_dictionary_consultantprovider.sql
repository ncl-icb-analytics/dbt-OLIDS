-- Staging model for dictionary.ConsultantProvider
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ConsultantID" as sk_consultantid,
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_SpecialtyID" as sk_specialtyid,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'ConsultantProvider') }}
