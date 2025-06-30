-- Staging model for CODESETS.SOURCE_CONCEPT_ORIGINS
-- Source: "DATA_LAB_OLIDS_UAT".REFERENCE

SELECT
    "SOURCE_CODE_ID_VALUE" AS source_code_id_value,
    "ORIGINATING_SOURCE_TABLE" AS originating_source_table
FROM {{ source('REFERENCE', 'SOURCE_CONCEPT_ORIGINS') }}
