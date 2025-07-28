/*
Flu Eligibility Comparison View

This simple comparison model demonstrates how to compare different campaigns
using the new simplified approach. Instead of complex hardcoded unions,
it shows how to use variables to switch between campaigns.

Usage Examples:
- Compare 2024-25 vs 2025-26: Run with different flu_current_campaign values
- Trend analysis: Run the base model for each campaign year
- Rule impact analysis: Compare outputs after rule changes

To compare campaigns:
1. Run base model with campaign A: dbt run --vars '{"flu_current_campaign": "flu_2024_25"}'
2. Run base model with campaign B: dbt run --vars '{"flu_current_campaign": "flu_2025_26"}'  
3. Use this view to union results for analysis

This approach is much simpler than the previous hardcoded comparison tables.
*/

{{ config(materialized='view') }}

-- Example: Compare current and previous campaigns
-- In practice, you would run the base model with different campaign variables

SELECT 
    campaign_id,
    rule_group_id,
    rule_group_name,
    rule_type,
    eligibility_priority,
    COUNT(*) as eligible_people,
    MIN(age_years) as min_age,
    MAX(age_years) as max_age,
    AVG(age_years) as avg_age
FROM {{ ref('fct_flu_eligibility') }}
GROUP BY campaign_id, rule_group_id, rule_group_name, rule_type, eligibility_priority
ORDER BY campaign_id, eligibility_priority, rule_group_id