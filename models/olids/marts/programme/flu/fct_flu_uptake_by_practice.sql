/*
Flu Vaccination Uptake by Practice (Aggregated)

This model aggregates flu vaccination uptake at the practice level,
providing key metrics for performance monitoring and comparison.

Key metrics:
- Total eligible population by practice
- Vaccination counts and rates
- Declination rates
- Coverage gaps
- Demographic breakdowns
- Comparison across campaigns

Usage:
- Monitor practice performance
- Compare uptake across PCNs and boroughs
- Identify practices with low uptake for targeted interventions
- Track improvement over time between campaigns
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'practice_code']
) }}

WITH practice_uptake AS (
    SELECT 
        campaign_id,
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        practice_borough,
        practice_neighbourhood,
        
        -- Population counts
        COUNT(DISTINCT person_id) AS total_population,
        COUNT(DISTINCT CASE WHEN is_eligible THEN person_id END) AS eligible_population,
        COUNT(DISTINCT CASE WHEN NOT is_eligible AND vaccinated THEN person_id END) AS non_eligible_vaccinated,
        
        -- Vaccination counts by status
        COUNT(DISTINCT CASE WHEN is_eligible AND vaccinated THEN person_id END) AS eligible_vaccinated,
        COUNT(DISTINCT CASE WHEN is_eligible AND declined THEN person_id END) AS eligible_declined,
        COUNT(DISTINCT CASE WHEN is_eligible AND eligible_no_record THEN person_id END) AS eligible_no_record,
        
        -- LAIV specific
        COUNT(DISTINCT CASE WHEN is_eligible AND laiv_given = 1 THEN person_id END) AS laiv_given_count,
        
        -- Demographic breakdowns for eligible population
        -- Age groups
        COUNT(DISTINCT CASE WHEN is_eligible AND age < 5 THEN person_id END) AS eligible_under_5,
        COUNT(DISTINCT CASE WHEN is_eligible AND age BETWEEN 5 AND 17 THEN person_id END) AS eligible_5_to_17,
        COUNT(DISTINCT CASE WHEN is_eligible AND age BETWEEN 18 AND 64 THEN person_id END) AS eligible_18_to_64,
        COUNT(DISTINCT CASE WHEN is_eligible AND age >= 65 THEN person_id END) AS eligible_65_plus,
        
        -- Vaccinated by age groups
        COUNT(DISTINCT CASE WHEN is_eligible AND vaccinated AND age < 5 THEN person_id END) AS vaccinated_under_5,
        COUNT(DISTINCT CASE WHEN is_eligible AND vaccinated AND age BETWEEN 5 AND 17 THEN person_id END) AS vaccinated_5_to_17,
        COUNT(DISTINCT CASE WHEN is_eligible AND vaccinated AND age BETWEEN 18 AND 64 THEN person_id END) AS vaccinated_18_to_64,
        COUNT(DISTINCT CASE WHEN is_eligible AND vaccinated AND age >= 65 THEN person_id END) AS vaccinated_65_plus,
        
        -- Primary eligibility categories
        COUNT(DISTINCT CASE WHEN is_eligible AND campaign_category = 'Age-Based' THEN person_id END) AS eligible_age_based,
        COUNT(DISTINCT CASE WHEN is_eligible AND campaign_category = 'Clinical Condition' THEN person_id END) AS eligible_clinical_condition,
        
        -- Time to vaccination metrics (for vaccinated eligible patients)
        AVG(CASE WHEN is_eligible AND vaccinated THEN days_to_vaccination END) AS avg_days_to_vaccination,
        MEDIAN(CASE WHEN is_eligible AND vaccinated THEN days_to_vaccination END) AS median_days_to_vaccination,
        MIN(CASE WHEN is_eligible AND vaccinated THEN days_to_vaccination END) AS min_days_to_vaccination,
        MAX(CASE WHEN is_eligible AND vaccinated THEN days_to_vaccination END) AS max_days_to_vaccination
        
    FROM {{ ref('fct_flu_uptake') }}
    WHERE practice_code IS NOT NULL  -- Exclude patients without practice registration
    GROUP BY 
        campaign_id,
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        practice_borough,
        practice_neighbourhood
),

practice_metrics AS (
    SELECT 
        *,
        
        -- Calculate rates
        ROUND(eligible_vaccinated * 100.0 / NULLIF(eligible_population, 0), 1) AS uptake_rate,
        ROUND(eligible_declined * 100.0 / NULLIF(eligible_population, 0), 1) AS declination_rate,
        ROUND(eligible_no_record * 100.0 / NULLIF(eligible_population, 0), 1) AS no_record_rate,
        ROUND(laiv_given_count * 100.0 / NULLIF(eligible_vaccinated, 0), 1) AS laiv_proportion,
        
        -- Age-specific uptake rates
        ROUND(vaccinated_under_5 * 100.0 / NULLIF(eligible_under_5, 0), 1) AS uptake_rate_under_5,
        ROUND(vaccinated_5_to_17 * 100.0 / NULLIF(eligible_5_to_17, 0), 1) AS uptake_rate_5_to_17,
        ROUND(vaccinated_18_to_64 * 100.0 / NULLIF(eligible_18_to_64, 0), 1) AS uptake_rate_18_to_64,
        ROUND(vaccinated_65_plus * 100.0 / NULLIF(eligible_65_plus, 0), 1) AS uptake_rate_65_plus,
        
        -- Coverage gap
        eligible_population - eligible_vaccinated AS coverage_gap,
        
        -- Performance indicators
        CASE 
            WHEN eligible_vaccinated * 100.0 / NULLIF(eligible_population, 0) >= 75 THEN 'High'
            WHEN eligible_vaccinated * 100.0 / NULLIF(eligible_population, 0) >= 50 THEN 'Medium'
            WHEN eligible_vaccinated * 100.0 / NULLIF(eligible_population, 0) >= 25 THEN 'Low'
            ELSE 'Very Low'
        END AS uptake_performance,
        
        -- List size category
        CASE 
            WHEN total_population >= 15000 THEN 'Very Large (15k+)'
            WHEN total_population >= 10000 THEN 'Large (10-15k)'
            WHEN total_population >= 5000 THEN 'Medium (5-10k)'
            WHEN total_population >= 2500 THEN 'Small (2.5-5k)'
            ELSE 'Very Small (<2.5k)'
        END AS practice_size_category,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM practice_uptake
),

-- Add campaign information
final_metrics AS (
    SELECT 
        pm.*,
        cc.campaign_name,
        cc.campaign_start_date,
        cc.campaign_reference_date,
        cc.audit_end_date
    FROM practice_metrics pm
    LEFT JOIN (
        SELECT DISTINCT 
            campaign_id, 
            campaign_name,
            campaign_start_date, 
            campaign_reference_date, 
            audit_end_date
        FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
        UNION ALL
        SELECT DISTINCT 
            campaign_id,
            campaign_name,
            campaign_start_date, 
            campaign_reference_date, 
            audit_end_date  
        FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
    ) cc
        ON pm.campaign_id = cc.campaign_id
)

SELECT * FROM final_metrics
ORDER BY campaign_id, practice_borough, pcn_code, practice_code