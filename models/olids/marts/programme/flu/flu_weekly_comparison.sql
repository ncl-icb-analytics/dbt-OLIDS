/*
Flu Weekly Campaign Comparison View

Provides side-by-side comparison of current vs previous flu campaign
at equivalent time points, handling campaign end dates correctly.

Key features:
- Week-aligned comparison (Week 1 vs Week 1, etc.)
- Shows "ahead or behind" metrics
- Handles incomplete current season vs complete previous season
- Supports both organisational and person-level comparisons
- Automatically adjusts based on campaign status (active vs ended)

This view enables answering the key question: "Are we ahead of or behind
where we were at this point last year?"
*/

{{ config(
    materialized='view'
) }}

WITH campaign_status AS (
    -- Determine current campaign status
    SELECT 
        cc.campaign_id,
        cc.campaign_name,
        cc.campaign_start_date,
        cc.campaign_end_date,
        cc.campaign_reference_date,
        CASE 
            WHEN CURRENT_DATE > cc.campaign_reference_date THEN 'COMPLETED'
            WHEN CURRENT_DATE > cc.campaign_end_date THEN 'COMPLETED'
            WHEN CURRENT_DATE >= cc.campaign_start_date THEN 'ACTIVE'
            ELSE 'FUTURE'
        END AS campaign_status,
        CASE 
            WHEN CURRENT_DATE > cc.campaign_reference_date THEN cc.campaign_reference_date
            WHEN CURRENT_DATE > cc.campaign_end_date THEN cc.campaign_end_date
            ELSE CURRENT_DATE
        END AS effective_comparison_date,
        LEAST(
            DATEDIFF('week', cc.campaign_start_date, 
                CASE 
                    WHEN CURRENT_DATE > cc.campaign_reference_date THEN cc.campaign_reference_date
                    WHEN CURRENT_DATE > cc.campaign_end_date THEN cc.campaign_end_date
                    ELSE CURRENT_DATE
                END
            ) + 1,
            26
        ) AS current_week_position
    FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }}) cc
    
    UNION ALL
    
    SELECT 
        cc.campaign_id,
        cc.campaign_name,
        cc.campaign_start_date,
        cc.campaign_end_date,
        cc.campaign_reference_date,
        'HISTORICAL' AS campaign_status,
        cc.campaign_reference_date AS effective_comparison_date,
        LEAST(DATEDIFF('week', cc.campaign_start_date, cc.campaign_reference_date) + 1, 26) AS current_week_position
    FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }}) cc
),

-- Get current campaign data (with risk group breakdown)
current_campaign AS (
    SELECT 
        org.week_number,
        org.week_start_date,
        org.practice_code,
        org.practice_name,
        org.pcn_name,
        org.practice_neighbourhood,
        org.practice_borough,
        org.risk_group,
        org.campaign_category,
        org.eligible_count,
        org.cumulative_vaccination_count,
        org.uptake_rate_percent,
        org.weekly_vaccination_count,
        cs.campaign_status,
        cs.current_week_position,
        cs.effective_comparison_date
    FROM {{ ref('fct_flu_weekly_org') }} org
    JOIN campaign_status cs 
        ON org.campaign_id = cs.campaign_id
    WHERE org.campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
),

-- Get previous campaign data (with risk group breakdown)
previous_campaign AS (
    SELECT 
        org.week_number,
        org.practice_code,
        org.risk_group,
        org.campaign_category,
        org.eligible_count AS prev_eligible_count,
        org.cumulative_vaccination_count AS prev_cumulative_vaccination_count,
        org.uptake_rate_percent AS prev_uptake_rate_percent,
        org.weekly_vaccination_count AS prev_weekly_vaccination_count
    FROM {{ ref('fct_flu_weekly_org') }} org
    WHERE org.campaign_id = '{{ var('flu_previous_campaign', 'flu_2023_24') }}'
),

-- Combine current and previous data
combined_comparison AS (
    SELECT 
        curr.week_number,
        curr.week_start_date,
        
        -- Practice hierarchy (smallest to largest unit)
        curr.practice_code,
        curr.practice_name,
        curr.pcn_name,
        curr.practice_neighbourhood,
        curr.practice_borough,
        
        -- Risk group dimensions
        curr.risk_group,
        curr.campaign_category,
        
        -- Current campaign metrics
        curr.eligible_count AS current_eligible,
        curr.cumulative_vaccination_count AS current_cumulative_vaccinations,
        curr.uptake_rate_percent AS current_uptake_rate,
        curr.weekly_vaccination_count AS current_weekly_vaccinations,
        
        -- Previous season metrics (same week position, previous campaign)
        prev.prev_eligible_count AS previous_season_eligible,
        prev.prev_cumulative_vaccination_count AS previous_season_cumulative_vaccinations,
        prev.prev_uptake_rate_percent AS previous_season_uptake_rate,
        prev.prev_weekly_vaccination_count AS previous_season_weekly_vaccinations,
        
        -- Year-over-year comparison metrics (current season vs previous season)
        curr.cumulative_vaccination_count - prev.prev_cumulative_vaccination_count AS vaccinations_vs_previous_season,
        curr.uptake_rate_percent - prev.prev_uptake_rate_percent AS uptake_rate_difference_vs_previous_season,
        
        -- Year-over-year percentage change
        CASE 
            WHEN prev.prev_cumulative_vaccination_count > 0
            THEN ROUND(
                100.0 * (curr.cumulative_vaccination_count - prev.prev_cumulative_vaccination_count) 
                / prev.prev_cumulative_vaccination_count, 
                1
            )
            ELSE NULL
        END AS percentage_change_vs_previous_season,
        
        -- Year-over-year performance indicators
        CASE 
            WHEN curr.cumulative_vaccination_count > prev.prev_cumulative_vaccination_count THEN 'AHEAD'
            WHEN curr.cumulative_vaccination_count < prev.prev_cumulative_vaccination_count THEN 'BEHIND'
            ELSE 'EQUAL'
        END AS position_vs_previous_season,
        
        CASE 
            WHEN curr.uptake_rate_percent > prev.prev_uptake_rate_percent THEN 'HIGHER'
            WHEN curr.uptake_rate_percent < prev.prev_uptake_rate_percent THEN 'LOWER'
            ELSE 'EQUAL'
        END AS uptake_rate_vs_previous_season,
        
        -- Campaign context
        curr.campaign_status,
        curr.current_week_position,
        curr.effective_comparison_date,
        
        -- Comparison validity
        CASE 
            WHEN curr.campaign_status = 'COMPLETED' THEN 'Final vs Final'
            WHEN curr.week_number <= curr.current_week_position THEN 'Week ' || curr.week_number || ' Comparison'
            ELSE 'Future Week - No Data'
        END AS comparison_type,
        
        -- Trajectory indicators (for incomplete current campaign)
        CASE 
            WHEN curr.campaign_status = 'ACTIVE' AND curr.week_number = curr.current_week_position
            THEN CASE 
                WHEN curr.cumulative_vaccination_count > prev.prev_cumulative_vaccination_count * 1.05 THEN 'Strong'
                WHEN curr.cumulative_vaccination_count > prev.prev_cumulative_vaccination_count THEN 'Good'
                WHEN curr.cumulative_vaccination_count > prev.prev_cumulative_vaccination_count * 0.95 THEN 'On Track'
                ELSE 'Concerning'
            END
            ELSE NULL
        END AS current_trajectory,
        
        CURRENT_TIMESTAMP AS created_at
        
    FROM current_campaign curr
    LEFT JOIN previous_campaign prev
        ON curr.week_number = prev.week_number
        AND curr.practice_code = prev.practice_code
        AND curr.risk_group = prev.risk_group
        AND curr.campaign_category = prev.campaign_category
),

-- Add summary statistics
practice_summary AS (
    SELECT 
        *,
        -- Week-specific rankings
        RANK() OVER (
            PARTITION BY week_number 
            ORDER BY current_cumulative_vaccinations DESC
        ) AS practice_rank_current_week,
        
        RANK() OVER (
            PARTITION BY week_number 
            ORDER BY vaccinations_vs_previous_season DESC
        ) AS practice_rank_vs_previous_season,
        
        -- PCN and Borough aggregations for context
        SUM(current_cumulative_vaccinations) OVER (
            PARTITION BY week_number, pcn_name
        ) AS pcn_total_current,
        
        SUM(previous_season_cumulative_vaccinations) OVER (
            PARTITION BY week_number, pcn_name
        ) AS pcn_total_previous_season,
        
        SUM(current_cumulative_vaccinations) OVER (
            PARTITION BY week_number, practice_borough
        ) AS borough_total_current,
        
        SUM(previous_season_cumulative_vaccinations) OVER (
            PARTITION BY week_number, practice_borough
        ) AS borough_total_previous_season
        
    FROM combined_comparison
)

SELECT * FROM practice_summary
ORDER BY week_number ASC, practice_code, risk_group, campaign_category