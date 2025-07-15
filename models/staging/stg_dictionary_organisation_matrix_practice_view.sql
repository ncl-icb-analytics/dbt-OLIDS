-- Staging model for Dictionary.dbo.OrganisationMatrixPracticeView
-- Source: Dictionary.dbo

SELECT
    "SK_OrganisationID_Practice" AS sk_organisation_id_practice,
    "PracticeCode" AS practice_code,
    "PracticeName" AS practice_name,
    "SK_OrganisationID_Network" AS sk_organisation_id_network,
    "NetworkCode" AS network_code,
    "NetworkName" AS network_name,
    "SK_OrganisationID_Commissioner" AS sk_organisation_id_commissioner,
    "CommissionerCode" AS commissioner_code,
    "CommissionerName" AS commissioner_name,
    "SK_OrganisationID_STP" AS sk_organisation_id_stp,
    "STPCode" AS stp_code,
    "STPName" AS stp_name
FROM {{ source('Dictionary_dbo', 'OrganisationMatrixPracticeView') }}