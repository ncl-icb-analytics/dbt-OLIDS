-- Staging model for CODESETS.CHILDHOOD_IMMS_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "VACCINE" AS vaccine,
    "DOSE" AS dose,
    "PROPOSEDCLUSTER" AS proposedcluster,
    "SOURCECLUSTERID" AS sourceclusterid,
    "SOURCECLUSTERDESCRIPTION" AS sourceclusterdescription,
    "SNOMEDCONCEPTID" AS snomedconceptid,
    "CODEDESCRIPTION" AS codedescription,
    "SOURCE" AS source
FROM {{ source('CODESETS', 'CHILDHOOD_IMMS_CODES') }}
