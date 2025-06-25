-- Staging model for CODESETS.CODESET_METADATA
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "FILE_TYPE" AS file_type,
    "LATEST_FILE_NAME" AS latest_file_name,
    "PROCESSED_DATE" AS processed_date,
    "FILE_DATE" AS file_date,
    "ROW_COUNT" AS row_count
FROM {{ source('CODESETS', 'CODESET_METADATA') }}
