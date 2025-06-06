-- Staging model for CODESETS.CHILDHOOD_IMMS_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "VACCINE" as vaccine,
    "DOSE" as dose,
    "PROPOSEDCLUSTER" as proposedcluster,
    "SOURCECLUSTERID" as sourceclusterid,
    "SOURCECLUSTERDESCRIPTION" as sourceclusterdescription,
    "SNOMEDCONCEPTID" as snomedconceptid,
    "CODEDESCRIPTION" as codedescription,
    "SOURCE" as source
from {{ source('CODESETS', 'CHILDHOOD_IMMS_CODES') }}
