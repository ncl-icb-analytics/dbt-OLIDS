-- Staging model for CODESETS.BNF_LATEST
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "PRESENTATION_PACK_LEVEL" AS presentation_pack_level,
    "VMP_VMPP_AMP_AMPP" AS vmp_vmpp_amp_ampp,
    "BNF_CODE" AS bnf_code,
    "BNF_NAME" AS bnf_name,
    "SNOMED_CODE" AS snomed_code,
    "DM_D_PRODUCT_DESCRIPTION" AS dm_d_product_description,
    "STRENGTH" AS strength,
    "UNIT_OF_MEASURE" AS unit_of_measure,
    "DM_D_PRODUCT_PACK_DESCRIPTION" AS dm_d_product_pack_description,
    "PACK" AS pack,
    "SUB_PACK" AS sub_pack,
    "VTM" AS vtm,
    "VTM_NAME" AS vtm_name
FROM {{ source('REFERENCE', 'BNF_LATEST') }}
