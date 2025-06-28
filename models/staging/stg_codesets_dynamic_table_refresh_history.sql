-- Staging model for CODESETS.DYNAMIC_TABLE_REFRESH_HISTORY
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

SELECT
    "TABLE_NAME" AS table_name,
    "REFRESH_START_TIME" AS refresh_start_time,
    "REFRESH_END_TIME" AS refresh_end_time,
    "REFRESH_DURATION_MS" AS refresh_duration_ms,
    "CREDITS_USED" AS credits_used,
    "ROWS_PROCESSED" AS rows_processed,
    "NEW_DATA_DETECTED" AS new_data_detected,
    "REFRESH_STATUS" AS refresh_status,
    "REFRESH_ACTION" AS refresh_action,
    "REFRESH_TRIGGER" AS refresh_trigger,
    "STATISTICS" AS statistics
FROM {{ source('CODESETS', 'DYNAMIC_TABLE_REFRESH_HISTORY') }}
