-- Staging model for CODESETS.CHILDHOOD_IMMS_CODES
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "VACCINE" AS vaccine,
    "DOSE" AS dose,
    "PROPOSEDCLUSTER" AS proposedcluster,
    "SOURCECLUSTERID" AS sourceclusterid,
    "SOURCECLUSTERDESCRIPTION" AS sourceclusterdescription,
    "SNOMEDCONCEPTID" AS snomedconceptid,
    "CODEDESCRIPTION" AS codedescription,
    "SOURCE" AS source
FROM {{ source('REFERENCE', 'CHILDHOOD_IMMS_CODES') }}
