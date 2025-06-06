-- Staging model for CODESETS.SOURCE_CONCEPT_ORIGINS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "SOURCE_CODE_ID_VALUE" as source_code_id_value,
    "ORIGINATING_SOURCE_TABLE" as originating_source_table
from {{ source('CODESETS', 'SOURCE_CONCEPT_ORIGINS') }}
