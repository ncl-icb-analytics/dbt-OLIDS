-- Staging model for dictionary.GP
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_GPID" as sk_gpid,
    "GP_Code" as gp_code,
    "GP_Name" as gp_name,
    "Contact_Telephone_Number" as contact_telephone_number,
    "SK_OrganisationID_NationalGrouping" as sk_organisationid_nationalgrouping,
    "SK_OrganisationID_HealthAuthority" as sk_organisationid_healthauthority,
    "SK_OrganisationID_CurrentCareOrg" as sk_organisationid_currentcareorg,
    "SK_PostcodeID" as sk_postcodeid,
    "StartDate" as startdate,
    "EndDate" as enddate,
    "SK_OrganisationID_ParentOrg" as sk_organisationid_parentorg,
    "Join_Parent_Date" as join_parent_date,
    "Left_Parent_Date" as left_parent_date,
    "FirstCreated" as firstcreated,
    "LastUpdated" as lastupdated,
    "SK_GP_ID" as sk_gp_id,
    "SK_NationalGrouping_ID" as sk_nationalgrouping_id,
    "SK_HealthAuthority_ID" as sk_healthauthority_id,
    "SK_CurrentCareOrg_ID" as sk_currentcareorg_id,
    "SK_Postcode_ID" as sk_postcode_id,
    "SK_ParentOrg_ID" as sk_parentorg_id,
    "GMC_GivenName" as gmc_givenname,
    "GMC_Surname" as gmc_surname,
    "GMC_ReferenceNumber" as gmc_referencenumber
from {{ source('dictionary', 'GP') }}
