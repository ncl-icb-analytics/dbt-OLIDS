/*
Flu Vaccination Uptake Timeseries Analysis

This analysis tracks flu vaccination uptake by month and category for both campaign_ids (2023-24 and 2024-25),
showing year-over-year comparison.

Key features:
- Monthly aggregation of vaccination dates
- Breakdown by eligibility category 
- Comparison between flu_2023_24 and flu_2024_25 campaigns
- Shows uptake rate differences between seasons
*/

-- Monthly uptake by category for both campaigns
WITH monthly_vaccinations AS (
    SELECT 
        campaign_id,
        DATE_TRUNC('month', vaccination_date) AS vaccination_month,
        campaign_category,
        risk_group,
        COUNT(DISTINCT person_id) AS vaccinated_count
    FROM {{ ref('fct_flu_uptake') }}
    WHERE vaccinated = TRUE
        AND vaccination_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- Total eligible by category per campaign
eligible_totals AS (
    SELECT 
        campaign_id,
        campaign_category,
        risk_group,
        COUNT(DISTINCT person_id) AS eligible_count
    FROM {{ ref('fct_flu_uptake') }}
    WHERE is_eligible = TRUE
    GROUP BY 1, 2, 3
),

-- Calculate cumulative vaccinations
cumulative_vaccinations AS (
    SELECT 
        mv.campaign_id,
        mv.vaccination_month,
        mv.campaign_category,
        mv.risk_group,
        mv.vaccinated_count,
        SUM(mv.vaccinated_count) OVER (
            PARTITION BY mv.campaign_id, mv.campaign_category, mv.risk_group 
            ORDER BY mv.vaccination_month
        ) AS cumulative_vaccinated,
        et.eligible_count
    FROM monthly_vaccinations mv
    JOIN eligible_totals et
        ON mv.campaign_id = et.campaign_id
        AND mv.campaign_category = et.campaign_category
        AND mv.risk_group = et.risk_group
),

-- Add uptake rates
uptake_with_rates AS (
    SELECT 
        campaign_id,
        vaccination_month,
        campaign_category,
        risk_group,
        vaccinated_count AS monthly_vaccinations,
        cumulative_vaccinated,
        eligible_count,
        ROUND(100.0 * cumulative_vaccinated / NULLIF(eligible_count, 0), 2) AS cumulative_uptake_rate
    FROM cumulative_vaccinations
),

-- Pivot to compare campaigns side by side
comparison_base AS (
    SELECT 
        EXTRACT(MONTH FROM vaccination_month) AS month_number,
        MONTHNAME(vaccination_month) AS month_name,
        campaign_category,
        risk_group,
        
        -- 2023-24 campaign metrics
        MAX(CASE WHEN campaign_id = 'flu_2023_24' THEN monthly_vaccinations END) AS vaccinations_2023_24,
        MAX(CASE WHEN campaign_id = 'flu_2023_24' THEN cumulative_vaccinated END) AS cumulative_2023_24,
        MAX(CASE WHEN campaign_id = 'flu_2023_24' THEN eligible_count END) AS eligible_2023_24,
        MAX(CASE WHEN campaign_id = 'flu_2023_24' THEN cumulative_uptake_rate END) AS uptake_rate_2023_24,
        
        -- 2024-25 campaign metrics
        MAX(CASE WHEN campaign_id = 'flu_2024_25' THEN monthly_vaccinations END) AS vaccinations_2024_25,
        MAX(CASE WHEN campaign_id = 'flu_2024_25' THEN cumulative_vaccinated END) AS cumulative_2024_25,
        MAX(CASE WHEN campaign_id = 'flu_2024_25' THEN eligible_count END) AS eligible_2024_25,
        MAX(CASE WHEN campaign_id = 'flu_2024_25' THEN cumulative_uptake_rate END) AS uptake_rate_2024_25
        
    FROM uptake_with_rates
    GROUP BY 1, 2, 3, 4
),

-- Calculate year-over-year differences
final_comparison AS (
    SELECT 
        month_number,
        month_name,
        campaign_category,
        risk_group,
        
        -- 2023-24 metrics
        COALESCE(vaccinations_2023_24, 0) AS monthly_vaccinations_2023_24,
        COALESCE(cumulative_2023_24, 0) AS cumulative_vaccinations_2023_24,
        COALESCE(eligible_2023_24, 0) AS eligible_count_2023_24,
        COALESCE(uptake_rate_2023_24, 0) AS uptake_rate_2023_24,
        
        -- 2024-25 metrics
        COALESCE(vaccinations_2024_25, 0) AS monthly_vaccinations_2024_25,
        COALESCE(cumulative_2024_25, 0) AS cumulative_vaccinations_2024_25,
        COALESCE(eligible_2024_25, 0) AS eligible_count_2024_25,
        COALESCE(uptake_rate_2024_25, 0) AS uptake_rate_2024_25,
        
        -- Year-over-year differences
        COALESCE(vaccinations_2024_25, 0) - COALESCE(vaccinations_2023_24, 0) AS monthly_diff,
        COALESCE(cumulative_2024_25, 0) - COALESCE(cumulative_2023_24, 0) AS cumulative_diff,
        ROUND(COALESCE(uptake_rate_2024_25, 0) - COALESCE(uptake_rate_2023_24, 0), 2) AS uptake_rate_diff_pp
        
    FROM comparison_base
)

SELECT 
    month_number,
    month_name,
    campaign_category,
    risk_group,
    
    -- 2023-24 season
    monthly_vaccinations_2023_24,
    cumulative_vaccinations_2023_24,
    eligible_count_2023_24,
    uptake_rate_2023_24 || '%' AS uptake_rate_2023_24,
    
    -- 2024-25 season
    monthly_vaccinations_2024_25,
    cumulative_vaccinations_2024_25,
    eligible_count_2024_25,
    uptake_rate_2024_25 || '%' AS uptake_rate_2024_25,
    
    -- Comparison
    CASE 
        WHEN monthly_diff > 0 THEN '+' || monthly_diff
        ELSE monthly_diff::VARCHAR
    END AS monthly_vaccinations_diff,
    CASE 
        WHEN cumulative_diff > 0 THEN '+' || cumulative_diff
        ELSE cumulative_diff::VARCHAR
    END AS cumulative_vaccinations_diff,
    CASE 
        WHEN uptake_rate_diff_pp > 0 THEN '+' || uptake_rate_diff_pp || 'pp'
        WHEN uptake_rate_diff_pp < 0 THEN uptake_rate_diff_pp || 'pp'
        ELSE '0pp'
    END AS uptake_rate_diff
    
FROM final_comparison
WHERE month_number >= 9 OR month_number <= 3  -- Focus on flu season months (Sep-Mar)
ORDER BY 
    campaign_category,
    risk_group,
    CASE 
        WHEN month_number >= 9 THEN month_number - 9
        ELSE month_number + 4
    END