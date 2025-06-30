/*
Flu Campaign Dates Staging
Source: seeds/flu_campaign_dates.csv

Campaign-specific dates and lookback periods for flu programme rules.
These dates change annually when setting up new flu campaigns.

Key date types:
- start_dat: Campaign start date (usually Sept 1)
- ref_dat: Campaign reference date (usually March 31 following year)
- child_dat: Child reference date for school age calculations (usually Aug 31)
- audit_end_dat: Campaign end date for audit purposes (usually Feb 28)
- latest_since_date: Lookback dates for medication rules
- birth_start/birth_end: Birth date ranges for child groups
- latest_after_date: Vaccination given after this date
*/

SELECT 
    campaign_id,
    rule_group_id,
    date_type,
    date_value,
    description
FROM {{ env_var('SNOWFLAKE_TARGET_DATABASE', 'DATA_LAB_OLIDS_UAT') }}.DBT_DEV_REFERENCE.FLU_CAMPAIGN_DATES
ORDER BY campaign_id, rule_group_id, date_type