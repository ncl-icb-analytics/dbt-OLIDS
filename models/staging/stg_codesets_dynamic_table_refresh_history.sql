-- Staging model for CODESETS.DYNAMIC_TABLE_REFRESH_HISTORY
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "TABLE_NAME" as table_name,
    "REFRESH_START_TIME" as refresh_start_time,
    "REFRESH_END_TIME" as refresh_end_time,
    "REFRESH_DURATION_MS" as refresh_duration_ms,
    "CREDITS_USED" as credits_used,
    "ROWS_PROCESSED" as rows_processed,
    "NEW_DATA_DETECTED" as new_data_detected,
    "REFRESH_STATUS" as refresh_status,
    "REFRESH_ACTION" as refresh_action,
    "REFRESH_TRIGGER" as refresh_trigger,
    "STATISTICS" as statistics
from {{ source('CODESETS', 'DYNAMIC_TABLE_REFRESH_HISTORY') }}
