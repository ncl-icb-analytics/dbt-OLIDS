{{
    config(
        materialized='table',
        cluster_by=['programme_type', 'campaign_id', 'practice_code', 'person_id']
    )
}}

/*
COVID and Flu Dashboard Base Table

This model provides dashboard-ready data by combining the pure uptake facts
with demographics and practice information for the COVID and Flu Dashboard.

Key features:
- Joins fct_covid_flu_uptake with dim_person_demographics
- Provides complete demographic, geographic and practice dimensions
- Optimised clustering for dashboard query patterns
- Single source of truth for COVID/Flu dashboard consumption

Multi-Programme Support:
COVID Campaigns:
- covid_2024_autumn, covid_2025_spring, covid_2025_autumn

Flu Campaigns: 
- flu_2023_24, flu_2024_25, flu_2025_26

Usage:
- Primary table for COVID and Flu Dashboard in PowerBI/Tableau
- Filter by programme_type for programme-specific dashboards
- Use campaign_id for specific campaign analysis
- Demographic breakdowns for equity analysis
*/

WITH uptake_with_demographics AS (
    SELECT 
        -- Programme and campaign identifiers
        u.programme_type,
        u.campaign_id,
        u.campaign_year,
        u.campaign_season,
        u.person_id,
        
        -- Demographics from dim_person_demographics
        d.is_active,
        d.sex,
        d.age,
        d.age_band_5y,
        d.age_band_10y,
        d.age_band_nhs,
        d.ethnicity_category,
        d.ethnicity_subcategory,
        d.ethnicity_granular,
        d.main_language,
        d.language_type,
        d.interpreter_needed,
        d.interpreter_type,
        
        -- Geographic and deprivation
        d.imd_quintile_19,
        d.imd_decile_19,
        d.lsoa_code_21 as lsoa_code,
        
        -- Practice information
        d.practice_code,
        d.practice_name,
        d.pcn_code,
        d.pcn_name,
        d.borough_registered,
        d.neighbourhood_registered,
        
        -- School information from dim_person_age
        pa.age_school_stage AS school_year,
        pa.is_primary_school_age,
        pa.is_secondary_school_age,
        
        -- Housebound status from dim_person_housebound_status
        COALESCE(hs.is_housebound, FALSE) AS is_housebound,
        
        -- Eligibility information from uptake facts
        u.is_eligible,
        u.campaign_category,
        u.risk_group,
        u.eligibility_reason,
        u.rule_type,
        
        -- Vaccination information from uptake facts
        u.vaccination_status,
        u.vaccination_date,
        u.vaccination_status_reason,
        u.vaccinated_despite_ineligible,
        
        -- Uptake flags and metrics from uptake facts
        u.vaccinated,
        u.declined,
        u.eligible_no_record,
        u.uptake_category,
        u.days_to_vaccination,
        
        -- Programme-specific fields
        u.laiv_given,
        
        -- Campaign dates
        u.campaign_start_date,
        u.campaign_reference_date,
        u.audit_end_date,
        u.created_at
        
    FROM {{ ref('fct_covid_flu_uptake') }} u
    LEFT JOIN {{ ref('dim_person_demographics') }} d
        ON u.person_id = d.person_id
    LEFT JOIN {{ ref('dim_person_age') }} pa
        ON u.person_id = pa.person_id
    LEFT JOIN {{ ref('dim_person_housebound_status') }} hs
        ON u.person_id = hs.person_id
)

SELECT * FROM uptake_with_demographics
ORDER BY programme_type, campaign_id, practice_code, person_id