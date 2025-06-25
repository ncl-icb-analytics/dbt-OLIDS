-- Staging model for CODESETS.CODESET_HISTORY
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "HISTORY_ID" AS history_id,
    "FILE_TYPE" AS file_type,
    "FILENAME" AS filename,
    "PROCESSED_DATE" AS processed_date,
    "FILE_DATE" AS file_date,
    "ROW_COUNT" AS row_count,
    "EFFECTIVE_FROM" AS effective_from,
    "EFFECTIVE_TO" AS effective_to,
    "IS_CURRENT" AS is_current,
    "ARCHIVE_PATH" AS archive_path,
    "HASH_VALUE" AS hash_value
FROM {{ source('CODESETS', 'CODESET_HISTORY') }}
