-- Staging model for CODESETS.MAPPED_CONCEPTS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "SOURCE_CODE_ID" AS source_code_id,
    "ORIGINATING_SOURCE_TABLE" AS originating_source_table,
    "CONCEPT_ID" AS concept_id,
    "CONCEPT_SYSTEM" AS concept_system,
    "CONCEPT_CODE" AS concept_code,
    "CONCEPT_DISPLAY" AS concept_display,
    "CLUSTER_ID" AS cluster_id,
    "CLUSTER_DESCRIPTION" AS cluster_description,
    "CODE_DESCRIPTION" AS code_description,
    "SOURCE" AS source
FROM {{ source('CODESETS', 'MAPPED_CONCEPTS') }}
