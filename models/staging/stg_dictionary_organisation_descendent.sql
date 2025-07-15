-- Staging model for Dictionary.dbo.OrganisationDescendent
-- Source: Dictionary.dbo

SELECT
    "OrganisationCode_Child" AS organisation_code_child,
    "OrganisationPrimaryRole_Child" AS organisation_primary_role_child,
    "Path" AS path,
    "RelationshipStartDate" AS relationship_start_date
FROM {{ source('Dictionary_dbo', 'OrganisationDescendent') }}