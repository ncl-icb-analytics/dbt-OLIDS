-- Staging model for CODESETS.MAPPED_CONCEPTS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "SOURCE_CODE_ID" as source_code_id,
    "ORIGINATING_SOURCE_TABLE" as originating_source_table,
    "CONCEPT_ID" as concept_id,
    "CONCEPT_SYSTEM" as concept_system,
    "CONCEPT_CODE" as concept_code,
    "CONCEPT_DISPLAY" as concept_display,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "CODE_DESCRIPTION" as code_description,
    "SOURCE" as source
from {{ source('CODESETS', 'MAPPED_CONCEPTS') }}
