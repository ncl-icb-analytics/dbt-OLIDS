/*
Flu Person-Week Snapshot (Sparse)

This model creates a sparse person-week table for flu vaccination tracking.
Only stores "interesting" weeks where status changes occur or at boundaries.

Key features:
- Week 1 baseline for all eligible persons
- Vaccination week when status changes
- Final week (26) for complete comparison
- Handles multiple campaigns automatically
- ~2M rows instead of 156M for full person-week

The sparse approach dramatically reduces storage while maintaining
full analytical capability when expanded with window functions.
*/

{{ config(
    materialized='incremental',
    unique_key=['person_id', 'campaign_id', 'week_number'],
    cluster_by=['campaign_id', 'week_number', 'person_id'],
    on_schema_change='fail'
) }}

WITH campaign_configs AS (
    -- Get all campaign configurations with effective dates
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

effective_dates AS (
    -- Determine the effective end date for each campaign
    SELECT 
        campaign_id,
        campaign_start_date,
        campaign_end_date,
        campaign_reference_date,
        CASE 
            WHEN CURRENT_DATE > campaign_reference_date THEN campaign_reference_date
            WHEN CURRENT_DATE > campaign_end_date THEN campaign_end_date
            ELSE CURRENT_DATE
        END AS effective_end_date,
        -- Calculate number of weeks in campaign (up to effective end, max 26 weeks)
        LEAST(
            DATEDIFF('week', campaign_start_date, 
                LEAST(
                    CASE 
                        WHEN CURRENT_DATE > campaign_reference_date THEN campaign_reference_date
                        WHEN CURRENT_DATE > campaign_end_date THEN campaign_end_date
                        ELSE CURRENT_DATE
                    END,
                    campaign_reference_date
                )
            ) + 1,
            26
        ) AS max_week_number
    FROM campaign_configs
),

-- Get all eligible persons per campaign
eligible_persons AS (
    SELECT DISTINCT 
        e.campaign_id,
        e.person_id,
        -- Capture primary risk group for analysis
        FIRST_VALUE(e.risk_group) OVER (
            PARTITION BY e.campaign_id, e.person_id 
            ORDER BY e.eligibility_priority
        ) AS primary_risk_group,
        ed.campaign_start_date,
        ed.max_week_number
    FROM {{ ref('fct_flu_eligibility') }} e
    JOIN effective_dates ed ON e.campaign_id = ed.campaign_id
),

-- Get vaccination events with week numbers
vaccination_events AS (
    SELECT 
        u.campaign_id,
        u.person_id,
        u.vaccination_date,
        LEAST(DATEDIFF('week', ed.campaign_start_date, u.vaccination_date) + 1, 26) AS vaccination_week,
        u.vaccination_status,
        ed.max_week_number
    FROM {{ ref('fct_flu_uptake') }} u
    JOIN effective_dates ed ON u.campaign_id = ed.campaign_id
    WHERE u.vaccinated = TRUE
        AND u.vaccination_date >= ed.campaign_start_date
        AND u.vaccination_date <= ed.effective_end_date
        AND DATEDIFF('week', ed.campaign_start_date, u.vaccination_date) + 1 <= 26
),

-- Create sparse records
sparse_records AS (
    -- Week 1: Baseline for all eligible persons
    SELECT 
        campaign_id,
        person_id,
        1 AS week_number,
        DATE_TRUNC('week', campaign_start_date) AS week_start_date,
        'ELIGIBLE_NOT_VACCINATED' AS vaccination_status,
        FALSE AS is_vaccinated,
        primary_risk_group,
        'baseline' AS record_type
    FROM eligible_persons
    WHERE 1 <= max_week_number  -- Only if campaign has started
    
    UNION ALL
    
    -- Vaccination week: Status change records
    SELECT 
        ve.campaign_id,
        ve.person_id,
        ve.vaccination_week AS week_number,
        DATE_TRUNC('week', ve.vaccination_date) AS week_start_date,
        ve.vaccination_status,
        TRUE AS is_vaccinated,
        ep.primary_risk_group,  -- Get risk group from eligible persons
        'vaccination' AS record_type
    FROM vaccination_events ve
    JOIN eligible_persons ep ON ve.campaign_id = ep.campaign_id AND ve.person_id = ep.person_id
    WHERE ve.vaccination_week BETWEEN 1 AND ve.max_week_number
    
    UNION ALL
    
    -- Final week: End state for all eligible persons (only if different from week 1)
    SELECT 
        ep.campaign_id,
        ep.person_id,
        LEAST(ep.max_week_number, 26) AS week_number,
        DATEADD('week', LEAST(ep.max_week_number, 26) - 1, DATE_TRUNC('week', ep.campaign_start_date)) AS week_start_date,
        COALESCE(ve.vaccination_status, 'ELIGIBLE_NOT_VACCINATED') AS vaccination_status,
        CASE WHEN ve.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_vaccinated,
        ep.primary_risk_group,
        'final' AS record_type
    FROM eligible_persons ep
    LEFT JOIN vaccination_events ve 
        ON ep.campaign_id = ve.campaign_id 
        AND ep.person_id = ve.person_id
        AND ve.vaccination_week <= LEAST(ep.max_week_number, 26)
    WHERE LEAST(ep.max_week_number, 26) > 1  -- Only if campaign has multiple weeks
        AND LEAST(ep.max_week_number, 26) <= 26  -- Ensure within valid range
)

-- Final output with deduplication
SELECT 
    campaign_id,
    person_id,
    week_number,
    week_start_date,
    vaccination_status,
    is_vaccinated,
    COALESCE(
        primary_risk_group,
        FIRST_VALUE(primary_risk_group IGNORE NULLS) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    ) AS primary_risk_group,
    record_type,
    CURRENT_TIMESTAMP AS created_at
FROM sparse_records

{% if is_incremental() %}
    -- Only process new weeks since last run
    WHERE week_number > (
        SELECT COALESCE(MAX(week_number), 0)
        FROM {{ this }}
        WHERE campaign_id = sparse_records.campaign_id
    )
{% endif %}

-- Remove duplicates (keep most recent status for each person-week)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY campaign_id, person_id, week_number 
    ORDER BY is_vaccinated DESC, record_type DESC
) = 1