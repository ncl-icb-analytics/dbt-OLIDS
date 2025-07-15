-- Staging model for Dictionary.dbo.Organisation
-- Source: Dictionary.dbo

SELECT
    "SK_OrganisationID" AS sk_organisation_id,
    "Organisation_Code" AS organisation_code,
    "Organisation_Name" AS organisation_name,
    "SK_OrganisationTypeID" AS sk_organisation_type_id,
    "SK_PostcodeID" AS sk_postcode_id,
    "StartDate" AS start_date,
    "EndDate" AS end_date,
    "SK_OrganisationID_ParentOrg" AS sk_organisation_id_parent_org,
    "FirstCreated" AS first_created,
    "LastUpdated" AS last_updated,
    "SK_OrganisationStatusID" AS sk_organisation_status_id,
    "Address_Line_1" AS address_line_1,
    "Address_Line_2" AS address_line_2,
    "Address_Line_3" AS address_line_3,
    "Address_Line_4" AS address_line_4,
    "Address_Line_5" AS address_line_5
FROM {{ source('Dictionary_dbo', 'Organisation') }}