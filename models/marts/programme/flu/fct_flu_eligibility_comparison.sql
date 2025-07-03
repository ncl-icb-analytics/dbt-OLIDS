/*
Flu Vaccination Eligibility Comparison Fact Table

This model enables comparison between flu campaigns by unioning
campaign-specific fact models. It demonstrates the power of the
campaign-specific model approach for multi-year analysis.

Usage Examples:
- Year-over-year eligibility trends
- Impact of rule changes between campaigns
- Population health analysis across seasons
- Coverage and uptake comparisons

To add a new campaign year:
1. Create new campaign-specific fact model (copy existing)
2. Add UNION clause below for new campaign
3. Update variables in dbt_project.yml
*/

{{ config(
    materialized='table',
    persist_docs={"relation": true, "columns": true},
    cluster_by=['campaign_period', 'person_id'],
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: Flu Eligibility Comparison - Multi-campaign comparison fact table enabling year-over-year flu vaccination eligibility analysis.

Business Purpose:
• Enables comparison between flu campaigns for multi-year analysis and trend identification
• Supports year-over-year eligibility trends analysis and rule change impact assessment
• Provides foundation for population health analysis across flu seasons
• Facilitates coverage and uptake comparisons between campaign periods

Data Granularity:
• One row per person per eligible rule group per campaign period
• Currently covers 2024-25 campaign with framework for historical and future campaigns
• Contains campaign period classification (current, previous, upcoming) for temporal analysis
• Preserves all eligibility details from campaign-specific fact models

Key Features:
• Multi-campaign comparison capability with campaign period classification
• Extensible framework for adding new campaign years through union operations
• Campaign-specific model approach ensuring data stability and historical preservation
• Comprehensive eligibility analysis supporting programme planning and evaluation'"
    ]
) }}

-- Current campaign (2024-25)
SELECT 
    campaign_id,
    campaign_name,
    campaign_start_date,
    campaign_ref_date,
    audit_end_date,
    rule_group_id,
    rule_group_name,
    rule_type,
    person_id,
    qualifying_event_date,
    reference_date,
    eligibility_reason,
    birth_date_approx,
    age_months,
    age_years,
    created_at,
    days_since_qualifying_event,
    eligibility_priority,
    'current' AS campaign_period
FROM {{ ref('fct_flu_eligibility_2024_25') }}

/*
Future Campaign Expansions:

When previous year data becomes available, uncomment and update:

UNION ALL
SELECT 
    campaign_id,
    campaign_name,
    campaign_start_date,
    campaign_ref_date,
    audit_end_date,
    rule_group_id,
    rule_group_name,
    rule_type,
    person_id,
    qualifying_event_date,
    reference_date,
    eligibility_reason,
    birth_date_approx,
    age_months,
    age_years,
    created_at,
    days_since_qualifying_event,
    eligibility_priority,
    'previous' AS campaign_period
FROM fct_flu_eligibility_2023_24

When 2025-26 rules are announced and implemented, uncomment:

UNION ALL
SELECT 
    campaign_id,
    campaign_name,
    campaign_start_date,
    campaign_ref_date,
    audit_end_date,
    rule_group_id,
    rule_group_name,
    rule_type,
    person_id,
    qualifying_event_date,
    reference_date,
    eligibility_reason,
    birth_date_approx,
    age_months,
    age_years,
    created_at,
    days_since_qualifying_event,
    eligibility_priority,
    'upcoming' AS campaign_period
FROM fct_flu_eligibility_2025_26
*/

ORDER BY campaign_id, person_id, eligibility_priority