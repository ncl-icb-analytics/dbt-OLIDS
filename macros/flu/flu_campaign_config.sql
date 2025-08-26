/*
Flu Campaign Configuration - Single Source of Truth

This macro provides all campaign-specific dates and parameters in one place.
Instead of scattered hardcoded dates, everything is defined here clearly.

MULTI-CAMPAIGN SUPPORT:
All flu models work with any campaign year by changing the flu_current_campaign variable.

Available campaigns:
- 'flu_2023_24' - 2023-24 Flu Vaccination Campaign
- 'flu_2024_25' - 2024-25 Flu Vaccination Campaign (default)
- 'flu_2025_26' - 2025-26 Flu Vaccination Campaign

Usage Examples:
- Default campaign: {{ flu_campaign_config() }}
- Specific campaign: {{ flu_campaign_config('flu_2023_24') }}
- Via dbt_project.yml: Set flu_current_campaign variable, then run normally

Configuration in dbt_project.yml:
vars:
  flu_current_campaign: "flu_2024_25"     # Change this to switch campaigns
  flu_previous_campaign: "flu_2023_24"    # For comparison queries

CHILD AGE GROUPS:
- Preschool: Typically ages 2-3, but birth date ranges vary by campaign
- School Age: Typically ages 4-16, but birth date ranges vary by campaign
- Age-agnostic model names allow for consistent year-to-year comparisons
*/

{% macro flu_campaign_config(campaign_id='flu_2024_25') %}
    {%- if campaign_id == 'flu_2023_24' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            '2023-24 Flu Vaccination Campaign' AS campaign_name,
            
            -- Core campaign dates
            '2023-09-01'::DATE AS campaign_start_date,
            '2024-03-31'::DATE AS campaign_reference_date, 
            '2023-08-31'::DATE AS child_reference_date,
            '2024-03-31'::DATE AS campaign_end_date,
            
            -- Medication lookback dates
            '2022-09-01'::DATE AS asthma_medication_lookback_date,
            '2023-03-01'::DATE AS immuno_medication_lookback_date,
            
            -- Child age group birth date ranges (campaign-specific)
            '2019-09-01'::DATE AS child_preschool_birth_start,
            '2021-08-31'::DATE AS child_preschool_birth_end,
            '2007-09-01'::DATE AS child_school_age_birth_start,
            '2019-08-31'::DATE AS child_school_age_birth_end,
            
            -- Vaccination tracking dates
            '2023-08-31'::DATE AS flu_vaccination_after_date,
            '2023-08-31'::DATE AS laiv_vaccination_after_date,
            
            -- Current audit date
            CURRENT_DATE AS audit_end_date
    {%- elif campaign_id == 'flu_2024_25' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            '2024-25 Flu Vaccination Campaign' AS campaign_name,
            
            -- Core campaign dates
            '2024-09-01'::DATE AS campaign_start_date,
            '2025-03-31'::DATE AS campaign_reference_date, 
            '2024-08-31'::DATE AS child_reference_date,
            '2025-02-28'::DATE AS campaign_end_date,
            
            -- Medication lookback dates
            '2023-09-01'::DATE AS asthma_medication_lookback_date,
            '2024-03-01'::DATE AS immuno_medication_lookback_date,
            
            -- Child age group birth date ranges (campaign-specific)
            '2020-09-01'::DATE AS child_preschool_birth_start,
            '2022-08-31'::DATE AS child_preschool_birth_end,
            '2008-09-01'::DATE AS child_school_age_birth_start,
            '2020-08-31'::DATE AS child_school_age_birth_end,
            
            -- Vaccination tracking dates
            '2024-08-31'::DATE AS flu_vaccination_after_date,
            '2024-08-31'::DATE AS laiv_vaccination_after_date,
            
            -- Current audit date
            CURRENT_DATE AS audit_end_date
    {%- elif campaign_id == 'flu_2025_26' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            '2025-26 Flu Vaccination Campaign' AS campaign_name,
            
            -- Core campaign dates (shifted +1 year)
            '2025-09-01'::DATE AS campaign_start_date,
            '2026-03-31'::DATE AS campaign_reference_date,
            '2025-08-31'::DATE AS child_reference_date,
            '2026-02-28'::DATE AS campaign_end_date,
            
            -- Medication lookback dates (shifted +1 year)
            '2024-09-01'::DATE AS asthma_medication_lookback_date,
            '2025-03-01'::DATE AS immuno_medication_lookback_date,
            
            -- Child age group birth date ranges (campaign-specific)
            '2021-09-01'::DATE AS child_preschool_birth_start,
            '2023-08-31'::DATE AS child_preschool_birth_end,
            '2009-09-01'::DATE AS child_school_age_birth_start,
            '2021-08-31'::DATE AS child_school_age_birth_end,
            
            -- Vaccination tracking dates (shifted +1 year)
            '2025-08-31'::DATE AS flu_vaccination_after_date,
            '2025-08-31'::DATE AS laiv_vaccination_after_date,
            
            -- Current audit date
            CURRENT_DATE AS audit_end_date
    {%- else -%}
        -- Default to current campaign if unknown campaign_id
        {{ flu_campaign_config('flu_2024_25') }}
    {%- endif -%}
{% endmacro %}

/*
Helper macro to get a specific campaign date
Usage: {{ flu_get_campaign_date('campaign_reference_date') }}
*/
{% macro flu_get_campaign_date(date_name, campaign_id=none) %}
    {%- set campaign_id = campaign_id or var('flu_current_campaign', 'flu_2024_25') -%}
    (SELECT {{ date_name }} FROM ({{ flu_campaign_config(campaign_id) }}))
{% endmacro %}