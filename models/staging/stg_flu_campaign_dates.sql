/*
Flu Campaign Dates Staging
Generated from static configuration - matches flu_campaign_dates.csv exactly

Campaign-specific dates for flu vaccination programmes.
This contains all the dates that change each campaign year.
*/

{{ config(materialized='view') }}

WITH date_data AS (
    -- Row 2: Campaign-wide dates (ALL rule groups)
    SELECT 'flu_2024_25' AS campaign_id, 'ALL' AS rule_group_id, 'start_dat' AS date_type, '2024-09-01'::DATE AS date_value, 'Campaign start date' AS description
    UNION ALL
    -- Row 3
    SELECT 'flu_2024_25', 'ALL', 'ref_dat', '2025-03-31'::DATE, 'Campaign reference date'
    UNION ALL
    -- Row 4
    SELECT 'flu_2024_25', 'ALL', 'child_dat', '2024-08-31'::DATE, 'Child reference date for school age calculations'
    UNION ALL
    -- Row 5
    SELECT 'flu_2024_25', 'ALL', 'audit_end_dat', '2025-02-28'::DATE, 'Campaign end date for audit purposes'
    UNION ALL
    -- Row 6: AST_GROUP dates
    SELECT 'flu_2024_25', 'AST_GROUP', 'latest_since_date', '2023-09-01'::DATE, 'Asthma medication lookback date'
    UNION ALL
    -- Row 7: IMMUNO_GROUP dates
    SELECT 'flu_2024_25', 'IMMUNO_GROUP', 'latest_since_date', '2024-03-01'::DATE, 'Immunosuppression medication lookback date'
    UNION ALL
    -- Row 8: CHILD_2_3 birth date ranges
    SELECT 'flu_2024_25', 'CHILD_2_3', 'birth_start', '2020-09-01'::DATE, 'Birth date range start for 2-3 year olds'
    UNION ALL
    -- Row 9
    SELECT 'flu_2024_25', 'CHILD_2_3', 'birth_end', '2022-08-31'::DATE, 'Birth date range end for 2-3 year olds'
    UNION ALL
    -- Row 10: CHILD_4_16 birth date ranges
    SELECT 'flu_2024_25', 'CHILD_4_16', 'birth_start', '2008-09-01'::DATE, 'Birth date range start for 4-16 year olds'
    UNION ALL
    -- Row 11
    SELECT 'flu_2024_25', 'CHILD_4_16', 'birth_end', '2020-08-31'::DATE, 'Birth date range end for 4-16 year olds'
    UNION ALL
    -- Row 12: FLUVAX_GROUP dates
    SELECT 'flu_2024_25', 'FLUVAX_GROUP', 'latest_after_date', '2024-08-31'::DATE, 'Flu vaccination given after this date'
    UNION ALL
    -- Row 13: LAIV_GROUP dates
    SELECT 'flu_2024_25', 'LAIV_GROUP', 'latest_after_date', '2024-08-31'::DATE, 'LAIV vaccination given after this date'
)

SELECT 
    campaign_id,
    rule_group_id,
    date_type,
    date_value,
    description
FROM date_data
ORDER BY campaign_id, rule_group_id, date_type