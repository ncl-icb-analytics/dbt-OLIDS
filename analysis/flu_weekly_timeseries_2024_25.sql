/*
Simple Weekly Flu Vaccination Uptake Timeseries - Campaign 2024-25

This analysis shows weekly vaccination counts for the flu_2024_25 campaign
starting from September 1st, 2024.

Key features:
- Weekly aggregation of vaccination dates
- Single campaign focus (flu_2024_25)
- Simple cumulative tracking
*/

-- Weekly vaccination counts
WITH vaccination_weeks AS (
    SELECT 
        DATE_TRUNC('week', vaccination_date) AS week_start,
        COUNT(DISTINCT person_id) AS weekly_vaccinations
    FROM {{ ref('fct_flu_uptake') }}
    WHERE campaign_id = 'flu_2024_25'
        AND vaccinated = TRUE
        AND vaccination_date IS NOT NULL
        AND vaccination_date >= '2024-09-01'
    GROUP BY 1
),

-- Generate date spine for weeks from Sept 1st onwards
date_spine AS (
    SELECT 
        DATE_TRUNC('week', DATEADD('week', seq, '2024-09-01'::DATE)) AS week_start
    FROM TABLE(GENERATOR(ROWCOUNT => 40)) -- Approx 40 weeks to cover the season
    WHERE week_start <= CURRENT_DATE()
),

-- Combine with vaccination data
weekly_data AS (
    SELECT 
        ds.week_start,
        COALESCE(vw.weekly_vaccinations, 0) AS weekly_vaccinations,
        SUM(COALESCE(vw.weekly_vaccinations, 0)) OVER (
            ORDER BY ds.week_start
        ) AS cumulative_vaccinations
    FROM date_spine ds
    LEFT JOIN vaccination_weeks vw
        ON ds.week_start = vw.week_start
),

-- Total eligible for percentage calculations
total_eligible AS (
    SELECT 
        COUNT(DISTINCT person_id) AS total_eligible_count
    FROM {{ ref('fct_flu_uptake') }}
    WHERE campaign_id = 'flu_2024_25'
        AND is_eligible = TRUE
)

SELECT 
    week_start,
    TO_CHAR(week_start, 'DD/MM/YYYY') AS week_start_formatted,
    DATEDIFF('week', '2024-09-01', week_start) + 1 AS week_number,
    weekly_vaccinations,
    cumulative_vaccinations,
    ROUND(100.0 * cumulative_vaccinations / te.total_eligible_count, 2) AS cumulative_uptake_rate
FROM weekly_data wd
CROSS JOIN total_eligible te
WHERE week_start >= '2024-09-01'
ORDER BY week_start