-- Staging model for CODESETS.CODESET_HISTORY
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "HISTORY_ID" as history_id,
    "FILE_TYPE" as file_type,
    "FILENAME" as filename,
    "PROCESSED_DATE" as processed_date,
    "FILE_DATE" as file_date,
    "ROW_COUNT" as row_count,
    "EFFECTIVE_FROM" as effective_from,
    "EFFECTIVE_TO" as effective_to,
    "IS_CURRENT" as is_current,
    "ARCHIVE_PATH" as archive_path,
    "HASH_VALUE" as hash_value
from {{ source('CODESETS', 'CODESET_HISTORY') }}
