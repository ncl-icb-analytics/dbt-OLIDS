-- Staging model for CODESETS.UKHSA_FLU_LATEST
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "CODING_SCHEME" AS coding_scheme,
    "CODE_LIBRARY" AS code_library,
    "CODE_GROUP" AS code_group,
    "CODE_GROUP_DESCRIPTION" AS code_group_description,
    "SNOMED_CODE" AS snomed_code,
    "SNOMED_DESCRIPTION" AS snomed_description,
    "DATE_CREATED" AS date_created,
    "VALIDATED_SCTID" AS validated_sctid,
    "EMIS_ASTRX" AS emis_astrx,
    "UNNAMED_9" AS unnamed_9,
    "TPP_ASTRX" AS tpp_astrx
FROM {{ source('CODESETS', 'UKHSA_FLU_LATEST') }}
