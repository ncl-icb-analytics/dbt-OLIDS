-- Staging model for reference.UKHSA_FLU_LATEST
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CODING_SCHEME" as coding_scheme,
    "CODE_LIBRARY" as code_library,
    "CODE_GROUP" as code_group,
    "CODE_GROUP_DESCRIPTION" as code_group_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "DATE_CREATED" as date_created,
    "VALIDATED_SCTID" as validated_sctid,
    "EMIS_ASTRX" as emis_astrx,
    "UNNAMED_9" as unnamed_9,
    "TPP_ASTRX" as tpp_astrx
from {{ source('reference', 'UKHSA_FLU_LATEST') }}
