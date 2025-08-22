/*
Flu Weekly Organisational Timeseries

Practice-level weekly vaccination tracking that naturally supports
organisational hierarchy rollups in PowerBI.

Key features:
- Practice grain with PCN, neighbourhood, and borough attributes
- Weekly and cumulative vaccination counts
- Eligible population counts per practice
- Uptake rates calculated at practice level
- PowerBI can aggregate up the hierarchy: Practice → PCN → Neighbourhood → Borough

This table is optimised for organisational performance tracking and
executive dashboards showing progress by geographical/administrative units.
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'week_number', 'practice_code', 'risk_group']
) }}

WITH campaign_configs AS (
    -- Get all campaign configurations
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

effective_dates AS (
    -- Determine the effective end date for each campaign
    SELECT 
        campaign_id,
        campaign_name,
        campaign_start_date,
        campaign_end_date,
        campaign_reference_date,
        CASE 
            WHEN CURRENT_DATE > campaign_reference_date THEN campaign_reference_date
            WHEN CURRENT_DATE > campaign_end_date THEN campaign_end_date
            ELSE CURRENT_DATE
        END AS effective_end_date,
        -- Calculate max week number based on effective end
        DATEDIFF('week', campaign_start_date, 
            LEAST(
                CASE 
                    WHEN CURRENT_DATE > campaign_reference_date THEN campaign_reference_date
                    WHEN CURRENT_DATE > campaign_end_date THEN campaign_end_date
                    ELSE CURRENT_DATE
                END,
                campaign_reference_date
            )
        ) + 1 AS max_week_number
    FROM campaign_configs
),

-- Generate week spine for each campaign
week_spine AS (
    SELECT 
        ed.campaign_id,
        ed.campaign_name,
        seq.week_number,
        DATEADD('week', seq.week_number - 1, DATE_TRUNC('week', ed.campaign_start_date)) AS week_start_date,
        DATEADD('day', 6, DATEADD('week', seq.week_number - 1, DATE_TRUNC('week', ed.campaign_start_date))) AS week_end_date,
        ed.campaign_start_date,
        ed.effective_end_date,
        ed.max_week_number
    FROM effective_dates ed
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS week_number
        FROM TABLE(GENERATOR(ROWCOUNT => 26))
    ) seq
    WHERE seq.week_number <= ed.max_week_number
),

-- Get eligible counts by practice, risk group, and campaign category
practice_eligible AS (
    SELECT 
        e.campaign_id,
        d.practice_code,
        e.risk_group,
        e.campaign_category,
        COUNT(DISTINCT e.person_id) AS eligible_count
    FROM {{ ref('fct_flu_eligibility') }} e
    JOIN {{ ref('dim_person_demographics') }} d ON e.person_id = d.person_id
    WHERE d.is_active = TRUE
    GROUP BY 1, 2, 3, 4
),

-- Get weekly vaccination counts by practice, risk group, and campaign category
weekly_vaccinations AS (
    SELECT 
        u.campaign_id,
        d.practice_code,
        u.risk_group,
        u.campaign_category,
        DATEDIFF('week', cc.campaign_start_date, u.vaccination_date) + 1 AS week_number,
        COUNT(DISTINCT u.person_id) AS weekly_vaccination_count
    FROM {{ ref('fct_flu_uptake') }} u
    JOIN {{ ref('dim_person_demographics') }} d ON u.person_id = d.person_id
    JOIN campaign_configs cc ON u.campaign_id = cc.campaign_id
    JOIN effective_dates ed ON u.campaign_id = ed.campaign_id
    WHERE u.vaccinated = TRUE
        AND u.vaccination_date >= cc.campaign_start_date
        AND u.vaccination_date <= ed.effective_end_date
        AND d.is_active = TRUE
        AND u.risk_group IS NOT NULL  -- Ensure we have risk group info
    GROUP BY 1, 2, 3, 4, 5
),

-- Combine all data with risk group and campaign category breakdowns
practice_weekly AS (
    SELECT 
        ws.campaign_id,
        ws.campaign_name,
        ws.week_number,
        ws.week_start_date,
        ws.week_end_date,
        pe.practice_code,
        pe.risk_group,
        pe.campaign_category,
        pe.eligible_count,
        COALESCE(wv.weekly_vaccination_count, 0) AS weekly_vaccination_count,
        -- Calculate cumulative vaccinations by risk group
        SUM(COALESCE(wv.weekly_vaccination_count, 0)) OVER (
            PARTITION BY ws.campaign_id, pe.practice_code, pe.risk_group, pe.campaign_category
            ORDER BY ws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_vaccination_count
    FROM week_spine ws
    CROSS JOIN practice_eligible pe
    LEFT JOIN weekly_vaccinations wv
        ON ws.campaign_id = wv.campaign_id
        AND pe.practice_code = wv.practice_code
        AND pe.risk_group = wv.risk_group
        AND pe.campaign_category = wv.campaign_category
        AND ws.week_number = wv.week_number
    WHERE pe.campaign_id = ws.campaign_id
),

-- Add total distinct counts per practice (to avoid double-counting overlapping risk groups)
practice_totals AS (
    SELECT 
        e.campaign_id,
        d.practice_code,
        'Total' AS risk_group,
        'All Categories' AS campaign_category,
        COUNT(DISTINCT e.person_id) AS total_eligible_count
    FROM {{ ref('fct_flu_eligibility') }} e
    JOIN {{ ref('dim_person_demographics') }} d ON e.person_id = d.person_id
    WHERE d.is_active = TRUE
    GROUP BY 1, 2
),

weekly_vaccination_totals AS (
    SELECT 
        u.campaign_id,
        d.practice_code,
        'Total' AS risk_group,
        'All Categories' AS campaign_category,
        DATEDIFF('week', cc.campaign_start_date, u.vaccination_date) + 1 AS week_number,
        COUNT(DISTINCT u.person_id) AS weekly_vaccination_count
    FROM {{ ref('fct_flu_uptake') }} u
    JOIN {{ ref('dim_person_demographics') }} d ON u.person_id = d.person_id
    JOIN campaign_configs cc ON u.campaign_id = cc.campaign_id
    JOIN effective_dates ed ON u.campaign_id = ed.campaign_id
    WHERE u.vaccinated = TRUE
        AND u.vaccination_date >= cc.campaign_start_date
        AND u.vaccination_date <= ed.effective_end_date
        AND d.is_active = TRUE
    GROUP BY 1, 2, 3, 4, 5
),

practice_weekly_totals AS (
    SELECT 
        ws.campaign_id,
        ws.campaign_name,
        ws.week_number,
        ws.week_start_date,
        ws.week_end_date,
        pt.practice_code,
        pt.risk_group,
        pt.campaign_category,
        pt.total_eligible_count AS eligible_count,
        COALESCE(wvt.weekly_vaccination_count, 0) AS weekly_vaccination_count,
        -- Calculate cumulative vaccinations for totals
        SUM(COALESCE(wvt.weekly_vaccination_count, 0)) OVER (
            PARTITION BY ws.campaign_id, pt.practice_code
            ORDER BY ws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_vaccination_count
    FROM week_spine ws
    CROSS JOIN practice_totals pt
    LEFT JOIN weekly_vaccination_totals wvt
        ON ws.campaign_id = wvt.campaign_id
        AND pt.practice_code = wvt.practice_code
        AND ws.week_number = wvt.week_number
    WHERE pt.campaign_id = ws.campaign_id
),

-- Combine risk group breakdowns with totals
all_practice_weekly AS (
    SELECT * FROM practice_weekly
    UNION ALL
    SELECT * FROM practice_weekly_totals
),

-- Add practice hierarchy information from demographics
final_output AS (
    SELECT 
        pw.campaign_id,
        pw.campaign_name,
        pw.week_number,
        pw.week_start_date,
        pw.week_end_date,
        
        -- Practice level
        pw.practice_code,
        d.practice_name,
        
        -- Risk group and campaign category breakdowns
        pw.risk_group,
        pw.campaign_category,
        
        -- Organisational hierarchy (for PowerBI rollups)
        d.pcn_code,
        d.pcn_name,
        d.practice_neighbourhood,
        d.practice_borough,
        d.practice_lsoa,
        d.practice_msoa,
        
        -- Counts and rates
        pw.eligible_count,
        pw.weekly_vaccination_count,
        pw.cumulative_vaccination_count,
        
        -- Calculate uptake rate
        ROUND(
            100.0 * pw.cumulative_vaccination_count / NULLIF(pw.eligible_count, 0), 
            1
        ) AS uptake_rate_percent,
        
        -- Week-over-week change (by risk group)
        pw.cumulative_vaccination_count - LAG(pw.cumulative_vaccination_count, 1, 0) OVER (
            PARTITION BY pw.campaign_id, pw.practice_code, pw.risk_group, pw.campaign_category
            ORDER BY pw.week_number
        ) AS week_on_week_change,
        
        -- Remaining to vaccinate
        pw.eligible_count - pw.cumulative_vaccination_count AS remaining_eligible,
        
        -- Campaign progress indicators
        CASE 
            WHEN pw.week_number = 1 THEN 'Campaign Start'
            WHEN pw.week_number BETWEEN 8 AND 12 THEN 'Peak Period'
            WHEN pw.week_number > 20 THEN 'Campaign End'
            ELSE 'Active Period'
        END AS campaign_phase,
        
        -- Is this the latest week with data?
        CASE 
            WHEN pw.week_number = MAX(pw.week_number) OVER (PARTITION BY pw.campaign_id)
            THEN TRUE ELSE FALSE 
        END AS is_latest_week,
        
        CURRENT_TIMESTAMP AS created_at
        
    FROM all_practice_weekly pw
    LEFT JOIN (
        -- Get one representative row per practice for hierarchy info
        SELECT DISTINCT
            practice_code,
            practice_name,
            pcn_code,
            pcn_name,
            practice_neighbourhood,
            practice_borough,
            practice_lsoa,
            practice_msoa
        FROM {{ ref('dim_person_demographics') }}
        WHERE is_active = TRUE
    ) d ON pw.practice_code = d.practice_code
)

SELECT * FROM final_output
ORDER BY campaign_id DESC, week_number, practice_borough, practice_neighbourhood, practice_code, campaign_category, risk_group