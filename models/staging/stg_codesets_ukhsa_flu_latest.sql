{{
    config(
        persist_docs={"relation": true, "columns": true}
    )
}}

/*
    Staging model for UKHSA flu vaccination clinical codes
    
    Purpose: Standardizes and cleans UKHSA flu vaccination clinical codes for use in 
    flu programme eligibility determination and clinical decision support.
    
    Source: "DATA_LAB_OLIDS_UAT".REFERENCE.UKHSA_FLU_LATEST
    
    Business Context: UKHSA (UK Health Security Agency) provides standardized clinical codes
    for identifying patients eligible for flu vaccination based on clinical risk factors.
    These codes cover various clinical conditions, procedures, and medication classes that
    indicate increased flu vaccination priority.
*/

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
FROM {{ source('REFERENCE', 'UKHSA_FLU_LATEST') }}
