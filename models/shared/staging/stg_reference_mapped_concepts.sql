-- Staging model for reference.MAPPED_CONCEPTS
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

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
from {{ source('reference', 'MAPPED_CONCEPTS') }}
