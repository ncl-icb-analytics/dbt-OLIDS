-- Staging model for "dbo".Organisation
-- Source: "Dictionary"."dbo"

select
    "SK_OrganisationID" as sk_organisationid,
    "Organisation_Code" as organisation_code,
    "Organisation_Name" as organisation_name,
    "SK_OrganisationTypeID" as sk_organisationtypeid,
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
    "SK_PrescribingSettingID" as sk_prescribingsettingid,
    "SK_OrganisationStatusID" as sk_organisationstatusid,
    "Address_Line_1" as address_line_1,
    "Address_Line_2" as address_line_2,
    "Address_Line_3" as address_line_3,
    "Address_Line_4" as address_line_4,
    "Address_Line_5" as address_line_5
from {{ source('dictionary', 'Organisation') }}
