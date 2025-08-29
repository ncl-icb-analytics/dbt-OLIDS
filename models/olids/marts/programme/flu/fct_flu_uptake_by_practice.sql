/*
Flu Vaccination Uptake by Practice and Eligibility Criterion

This model provides one row per eligibility criterion per practice,
facilitating easier filtering and aggregation of uptake data.

Key features:
- One row per eligibility criterion (risk_group) and practice
- Includes a 'TOTAL' row per practice for overall metrics
- Simple structure for easy filtering and summing
- Focus on core uptake metrics without complex breakdowns

Usage:
- Filter by risk_group for specific eligibility criteria
- Use the 'TOTAL' rows to get practice-level summaries
- Easy aggregation to PCN or borough level
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'practice_code', 'eligibility_criterion']
) }}

WITH uptake_by_criterion AS (
    -- Get uptake metrics for each eligibility criterion and practice
    SELECT 
        fu.campaign_id,
        fu.practice_code,
        fu.practice_name,
        fu.pcn_code,
        fu.pcn_name,
        fu.practice_borough,
        fu.practice_neighbourhood,
        fe.risk_group AS eligibility_criterion,
        
        -- Population counts
        COUNT(DISTINCT fe.person_id) AS eligible_population,
        COUNT(DISTINCT CASE WHEN fu.vaccinated THEN fe.person_id END) AS vaccinated_count,
        COUNT(DISTINCT CASE WHEN fu.declined THEN fe.person_id END) AS declined_count,
        COUNT(DISTINCT CASE WHEN fu.eligible_no_record THEN fe.person_id END) AS no_record_count,
        
        -- LAIV specific
        COUNT(DISTINCT CASE WHEN fu.laiv_given = 1 THEN fe.person_id END) AS laiv_given_count
        
    FROM {{ ref('fct_flu_eligibility') }} fe
    INNER JOIN {{ ref('fct_flu_uptake') }} fu
        ON fe.campaign_id = fu.campaign_id
        AND fe.person_id = fu.person_id
    WHERE fu.practice_code IS NOT NULL  -- Exclude patients without practice registration
    GROUP BY 
        fu.campaign_id,
        fu.practice_code,
        fu.practice_name,
        fu.pcn_code,
        fu.pcn_name,
        fu.practice_borough,
        fu.practice_neighbourhood,
        fe.risk_group
),

practice_totals AS (
    -- Calculate overall practice totals (across all eligibility criteria)
    SELECT 
        campaign_id,
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        practice_borough,
        practice_neighbourhood,
        'TOTAL' AS eligibility_criterion,
        
        -- Population counts (distinct to avoid double counting across multiple criteria)
        COUNT(DISTINCT person_id) AS eligible_population,
        COUNT(DISTINCT CASE WHEN vaccinated THEN person_id END) AS vaccinated_count,
        COUNT(DISTINCT CASE WHEN declined THEN person_id END) AS declined_count,
        COUNT(DISTINCT CASE WHEN eligible_no_record THEN person_id END) AS no_record_count,
        COUNT(DISTINCT CASE WHEN laiv_given = 1 THEN person_id END) AS laiv_given_count
        
    FROM {{ ref('fct_flu_uptake') }}
    WHERE practice_code IS NOT NULL
        AND is_eligible = TRUE  -- Only count eligible patients for totals
    GROUP BY 
        campaign_id,
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        practice_borough,
        practice_neighbourhood
),

combined AS (
    SELECT * FROM uptake_by_criterion
    UNION ALL
    SELECT * FROM practice_totals
),

final AS (
    SELECT 
        campaign_id,
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        practice_borough,
        practice_neighbourhood,
        eligibility_criterion,
        
        -- Core counts
        eligible_population,
        vaccinated_count,
        declined_count,
        no_record_count,
        laiv_given_count,
        
        -- Calculate rates
        ROUND(vaccinated_count * 100.0 / NULLIF(eligible_population, 0), 1) AS uptake_rate,
        ROUND(declined_count * 100.0 / NULLIF(eligible_population, 0), 1) AS declination_rate,
        ROUND(no_record_count * 100.0 / NULLIF(eligible_population, 0), 1) AS no_record_rate,
        ROUND(laiv_given_count * 100.0 / NULLIF(vaccinated_count, 0), 1) AS laiv_proportion,
        
        -- Coverage gap
        eligible_population - vaccinated_count AS coverage_gap,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM combined
)

SELECT * FROM final
ORDER BY 
    campaign_id,
    practice_code,
    CASE WHEN eligibility_criterion = 'TOTAL' THEN 0 ELSE 1 END,  -- TOTAL rows first
    eligibility_criterion