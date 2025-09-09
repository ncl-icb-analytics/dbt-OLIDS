/*
Combined COVID and Flu Vaccination Uptake Fact Table

This model combines uptake data from both COVID and flu vaccination programmes 
to support integrated analysis and the COVID and Flu Dashboard.

Key features:
- Unified schema combining both COVID and flu uptake data
- Programme identifier to distinguish between COVID and flu records
- Consistent column structures enabling cross-programme analysis
- Support for year-over-year and programme-to-programme comparison
- Focused on uptake business process without demographics

Multi-Programme Support:
COVID Campaigns:
- covid_2024_autumn, covid_2025_spring, covid_2025_autumn

Flu Campaigns: 
- flu_2023_24, flu_2024_25, flu_2025_26

Usage:
- Filter by programme_type for programme-specific analysis
- Use campaign_id for specific campaign analysis
- Join with published_reporting_direct_care.covid_flu_dashboard_base for demographics
- Year-over-year comparison across programmes
*/

{{ config(
    materialized='table',
    cluster_by=['programme_type', 'campaign_id', 'person_id']
) }}

WITH covid_uptake AS (
    -- COVID vaccination uptake data
    SELECT 
        'COVID' AS programme_type,
        campaign_id,
        person_id,
        
        -- Eligibility information
        is_eligible,
        campaign_category,
        risk_group,
        eligibility_reason,
        rule_type,
        
        -- Vaccination information
        vaccination_status,
        vaccination_date,
        vaccination_status_reason,
        vaccinated_despite_ineligible,
        
        -- Uptake flags
        vaccinated,
        declined,
        eligible_no_record,
        uptake_category,
        days_to_vaccination,
        
        -- Programme-specific fields (COVID doesn't have LAIV)
        FALSE AS laiv_given,
        
        -- Campaign dates
        campaign_start_date,
        campaign_reference_date,
        audit_end_date,
        created_at
        
    FROM {{ ref('fct_covid_uptake') }}
),

flu_uptake AS (
    -- Flu vaccination uptake data
    SELECT 
        'FLU' AS programme_type,
        campaign_id,
        person_id,
        
        -- Eligibility information
        is_eligible,
        campaign_category,
        risk_group,
        eligibility_reason,
        rule_type,
        
        -- Vaccination information
        vaccination_status,
        vaccination_date,
        vaccination_status_reason,
        vaccinated_despite_ineligible,
        
        -- Uptake flags
        vaccinated,
        declined,
        eligible_no_record,
        uptake_category,
        days_to_vaccination,
        
        -- Programme-specific fields (Flu has LAIV)
        CASE WHEN laiv_given = 1 THEN TRUE ELSE FALSE END AS laiv_given,
        
        -- Campaign dates
        campaign_start_date,
        campaign_reference_date,
        audit_end_date,
        created_at
        
    FROM {{ ref('fct_flu_uptake') }}
),

combined_uptake AS (
    -- Union COVID and flu data
    SELECT * FROM covid_uptake
    UNION ALL
    SELECT * FROM flu_uptake
),

final_combined AS (
    SELECT 
        programme_type,
        campaign_id,
        
        -- Extract campaign year for easier analysis
        CASE 
            WHEN programme_type = 'COVID' THEN
                CASE 
                    WHEN campaign_id IN ('covid_2024_autumn') THEN '2024/25'
                    WHEN campaign_id IN ('covid_2025_spring', 'covid_2025_autumn') THEN '2025/26'
                    ELSE 'Unknown'
                END
            WHEN programme_type = 'FLU' THEN
                CASE 
                    WHEN campaign_id = 'flu_2023_24' THEN '2023/24'
                    WHEN campaign_id = 'flu_2024_25' THEN '2024/25'
                    WHEN campaign_id = 'flu_2025_26' THEN '2025/26'
                    ELSE 'Unknown'
                END
            ELSE 'Unknown'
        END AS campaign_year,
        
        -- Extract campaign season for easier analysis
        CASE 
            WHEN programme_type = 'COVID' THEN
                CASE 
                    WHEN campaign_id LIKE '%autumn%' THEN 'Autumn'
                    WHEN campaign_id LIKE '%spring%' THEN 'Spring'
                    ELSE 'Unknown'
                END
            WHEN programme_type = 'FLU' THEN 'Annual'
            ELSE 'Unknown'
        END AS campaign_season,
        
        person_id,
        
        -- Eligibility information
        is_eligible, campaign_category, risk_group, eligibility_reason, rule_type,
        
        -- Vaccination information
        vaccination_status, vaccination_date, vaccination_status_reason, 
        vaccinated_despite_ineligible,
        
        -- Uptake flags
        vaccinated, declined, eligible_no_record, uptake_category, days_to_vaccination,
        
        -- Programme-specific fields
        laiv_given,
        
        -- Campaign dates
        campaign_start_date, campaign_reference_date, audit_end_date, created_at
        
    FROM combined_uptake
)

SELECT * FROM final_combined
ORDER BY programme_type, campaign_id, person_id