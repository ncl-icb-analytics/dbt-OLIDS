/*
Sample Queries for Flu Timeseries Models

These queries demonstrate the analytical capabilities of the new flu timeseries
models and serve as tests to ensure the models are working correctly.
*/

-- Query 1: Organisational Hierarchy - Borough Level Rollup
-- Shows current campaign progress by borough
SELECT 
    practice_borough,
    week_number,
    SUM(eligible_count) as total_eligible,
    SUM(cumulative_vaccination_count) as total_vaccinated,
    ROUND(100.0 * SUM(cumulative_vaccination_count) / SUM(eligible_count), 1) as borough_uptake_rate
FROM {{ ref('fct_flu_weekly_org') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
    AND is_latest_week = TRUE
GROUP BY practice_borough, week_number
ORDER BY borough_uptake_rate DESC;

-- Query 2: PCN Performance Comparison
-- Shows current vs previous year performance by PCN
SELECT 
    pcn_name,
    week_number,
    current_cumulative_vaccinations,
    previous_cumulative_vaccinations,
    vaccinations_ahead_behind,
    position_vs_last_year,
    current_trajectory
FROM {{ ref('flu_weekly_comparison') }}
WHERE week_number = (
    SELECT MAX(week_number) 
    FROM {{ ref('flu_weekly_comparison') }}
    WHERE comparison_type LIKE 'Week%'
)
ORDER BY vaccinations_ahead_behind DESC;

-- Query 3: Complex Demographic Filtering (PowerBI Use Case)
-- Shows uptake for diabetic patients over 65 by ethnicity
SELECT 
    ethnicity_category,
    practice_borough,
    COUNT(DISTINCT person_id) as eligible_diabetic_over_65,
    SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) as vaccinated_count,
    ROUND(100.0 * SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) / COUNT(DISTINCT person_id), 1) as uptake_rate
FROM {{ ref('fct_flu_weekly_person') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
    AND is_latest_week = TRUE
    AND has_diabetes = TRUE
    AND is_over_65 = TRUE
GROUP BY ethnicity_category, practice_borough
HAVING COUNT(DISTINCT person_id) >= 10  -- Privacy threshold
ORDER BY uptake_rate DESC;

-- Query 4: Weekly Vaccination Velocity by Practice
-- Shows weekly vaccination counts and cumulative progress
SELECT 
    practice_name,
    week_number,
    weekly_vaccination_count,
    cumulative_vaccination_count,
    uptake_rate_percent,
    campaign_phase
FROM {{ ref('fct_flu_weekly_org') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
    AND practice_code = 'M85001'  -- Example practice
ORDER BY week_number;

-- Query 5: Risk Group Analysis - Multiple Conditions
-- Shows uptake for people with multiple risk factors
SELECT 
    CASE 
        WHEN has_diabetes AND has_heart_disease THEN 'Diabetes + Heart Disease'
        WHEN has_diabetes AND has_respiratory_disease THEN 'Diabetes + Respiratory'
        WHEN has_diabetes THEN 'Diabetes Only'
        WHEN has_heart_disease THEN 'Heart Disease Only'
        WHEN has_respiratory_disease THEN 'Respiratory Only'
        ELSE 'Other Clinical'
    END as risk_group_combination,
    COUNT(DISTINCT person_id) as eligible_count,
    SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) as vaccinated_count,
    ROUND(100.0 * SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) / COUNT(DISTINCT person_id), 1) as uptake_rate
FROM {{ ref('fct_flu_weekly_person') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
    AND is_latest_week = TRUE
    AND is_clinical_eligible = TRUE
GROUP BY 1
ORDER BY uptake_rate DESC;

-- Query 6: Campaign Progress Timeline - Week by Week
-- Shows vaccination progress week by week for current campaign
SELECT 
    week_number,
    week_start_date,
    SUM(weekly_vaccination_count) as new_vaccinations_this_week,
    SUM(cumulative_vaccination_count) as total_vaccinations_to_date,
    SUM(eligible_count) as total_eligible,
    ROUND(100.0 * SUM(cumulative_vaccination_count) / SUM(eligible_count), 1) as cumulative_uptake_rate
FROM {{ ref('fct_flu_weekly_org') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
GROUP BY week_number, week_start_date
ORDER BY week_number;

-- Query 7: Deprivation Analysis
-- Shows uptake by deprivation decile
SELECT 
    imd_decile_19,
    COUNT(DISTINCT person_id) as eligible_count,
    SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) as vaccinated_count,
    ROUND(100.0 * SUM(CASE WHEN is_vaccinated THEN 1 ELSE 0 END) / COUNT(DISTINCT person_id), 1) as uptake_rate
FROM {{ ref('fct_flu_weekly_person') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
    AND is_latest_week = TRUE
    AND imd_decile_19 IS NOT NULL
GROUP BY imd_decile_19
ORDER BY imd_decile_19;

-- Query 8: Vaccination Timing Analysis
-- Shows when people got vaccinated during the campaign
SELECT 
    week_number,
    campaign_phase,
    COUNT(DISTINCT person_id) as people_vaccinated_this_week,
    SUM(COUNT(DISTINCT person_id)) OVER (ORDER BY week_number) as cumulative_people_vaccinated,
    ROUND(
        100.0 * COUNT(DISTINCT person_id) / 
        SUM(COUNT(DISTINCT person_id)) OVER (), 
        1
    ) as percentage_of_total_vaccinations
FROM {{ ref('fct_flu_weekly_person') }}
WHERE campaign_id = '{{ var('flu_current_campaign', 'flu_2024_25') }}'
    AND vaccination_occurred_this_week = TRUE
GROUP BY week_number, campaign_phase
ORDER BY week_number;

-- Query 9: Practice Performance Rankings
-- Shows practice rankings by improvement vs last year
SELECT 
    practice_name,
    pcn_name,
    practice_borough,
    current_cumulative_vaccinations,
    previous_cumulative_vaccinations,
    vaccinations_ahead_behind,
    percentage_change_vaccinations,
    practice_rank_improvement,
    current_trajectory
FROM {{ ref('flu_weekly_comparison') }}
WHERE week_number = (
    SELECT MAX(week_number) 
    FROM {{ ref('flu_weekly_comparison') }}
    WHERE comparison_type LIKE 'Week%'
)
    AND practice_rank_improvement <= 10  -- Top 10 improvers
ORDER BY practice_rank_improvement;

-- Query 10: Data Quality Check - Sparse Table Efficiency
-- Shows the efficiency of the sparse approach
SELECT 
    campaign_id,
    COUNT(*) as sparse_records,
    COUNT(DISTINCT person_id) as unique_persons,
    COUNT(DISTINCT week_number) as weeks_with_data,
    ROUND(COUNT(*) / (COUNT(DISTINCT person_id) * COUNT(DISTINCT week_number)), 2) as sparsity_ratio
FROM {{ ref('int_flu_person_week_snapshot') }}
GROUP BY campaign_id
ORDER BY campaign_id;