-- Staging model for reference.SOURCE_CONCEPT_ORIGINS
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "SOURCE_CODE_ID_VALUE" as source_code_id_value,
    "ORIGINATING_SOURCE_TABLE" as originating_source_table
from {{ source('reference', 'SOURCE_CONCEPT_ORIGINS') }}
