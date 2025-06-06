-- Staging model for CODESETS.CODESET_METADATA
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "FILE_TYPE" as file_type,
    "LATEST_FILE_NAME" as latest_file_name,
    "PROCESSED_DATE" as processed_date,
    "FILE_DATE" as file_date,
    "ROW_COUNT" as row_count
from {{ source('CODESETS', 'CODESET_METADATA') }}
