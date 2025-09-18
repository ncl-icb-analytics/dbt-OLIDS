{{
    config(
        materialized='view'
    )
}}

/*
GP practice weighted list size data.
Contains practice list sizes and normalised weighted list sizes by financial quarter.
*/

SELECT
    site,
    financial_quarter_date,
    pct,
    practice_code,
    practice_name,
    gms_pms_flag,
    commissioner_code,
    commissioner_name,
    practice_list_size,
    practice_normalised_weighted_list_size,
    report_execution_datetime

FROM {{ source('reference', 'GP_WEIGHTED_LIST_SIZE_LATEST') }}