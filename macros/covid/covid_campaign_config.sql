/*
COVID Campaign Configuration - Single Source of Truth

This macro provides all campaign-specific dates and parameters in one place.
Instead of scattered hardcoded dates, everything is defined here clearly.

MULTI-CAMPAIGN SUPPORT:
All COVID models work with any campaign by changing the covid_current_campaign variable.

Available campaigns:
- 'covid_2024_autumn' - Autumn 2024 COVID Vaccination Campaign
- 'covid_2025_spring' - Spring 2025 COVID Vaccination Campaign  
- 'covid_2025_autumn' - Autumn 2025 COVID Vaccination Campaign

Usage Examples:
- Default campaign: {{ covid_campaign_config() }}
- Specific campaign: {{ covid_campaign_config('covid_2024_autumn') }}
- Via dbt_project.yml: Set covid_current_campaign variable, then run normally

Configuration in dbt_project.yml:
vars:
  covid_current_campaign: "covid_2025_autumn"     # Change this to switch campaigns
  covid_previous_campaign: "covid_2024_autumn"    # For comparison queries

CAMPAIGN ELIGIBILITY DIFFERENCES:
- 2024/25: Broader eligibility (age 65+ autumn, clinical risk groups)
- 2025/26: Restricted eligibility (age 75+, immunosuppressed, care home 65+ only)

COMPLEX ASTHMA STEROID WINDOWS:
Three overlapping 2-year windows to capture repeated steroid use across campaign periods.
*/

{% macro covid_campaign_config(campaign_id='covid_2025_autumn') %}
    {%- if campaign_id == 'covid_2024_autumn' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            'Autumn 2024 COVID Vaccination Campaign' AS campaign_name,
            'covid_2024_25' AS campaign_year,
            'autumn' AS campaign_period,
            
            -- Core campaign dates
            '2024-09-01'::DATE AS campaign_start_date,
            '2025-03-31'::DATE AS campaign_end_date,
            '2025-03-31'::DATE AS campaign_reference_date,
            
            -- Medication lookback dates (from START_DAT)
            '2024-03-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2022-09-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start  
            '2022-09-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start
            '2021-09-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start
            
            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2022-09-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before autumn start
            '2024-08-31'::DATE AS asthma_steroid_window_1_end,          -- Up to autumn 2024
            '2023-04-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from spring 2023
            '2025-03-31'::DATE AS asthma_steroid_window_2_end,          -- Up to spring 2025  
            '2023-07-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2025-06-30'::DATE AS asthma_steroid_window_3_end,          -- Up to spring 2025 end
            
            -- Vaccination tracking dates
            '2024-09-01'::DATE AS vaccination_tracking_start,
            '2025-03-31'::DATE AS vaccination_tracking_end,
            '2024-08-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2025-06-30'::DATE AS decline_tracking_end,                 -- Through spring period
            
            -- Pregnancy tracking (campaign-specific) 
            '2024-01-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign
            '2024-09-01'::DATE AS pregnancy_current_start,              -- Campaign period start
            '2025-06-30'::DATE AS pregnancy_current_end,                -- Through spring period
            '2024-01-14'::DATE AS gestational_diabetes_start,          -- Gestational diabetes tracking
            
            
            -- Individual condition eligibility flags (2024/25 campaigns)
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,  
            TRUE AS eligible_care_home,
            TRUE AS eligible_asthma,
            TRUE AS eligible_chronic_heart_disease,
            TRUE AS eligible_chronic_kidney_disease,
            TRUE AS eligible_diabetes,
            TRUE AS eligible_chronic_liver_disease,
            TRUE AS eligible_chronic_neurological_disease,
            TRUE AS eligible_chronic_respiratory_disease,
            TRUE AS eligible_morbid_obesity,
            TRUE AS eligible_asplenia,
            TRUE AS eligible_learning_disability,
            TRUE AS eligible_severe_mental_illness,
            TRUE AS eligible_pregnancy,
            TRUE AS eligible_gestational_diabetes,
            TRUE AS eligible_homeless,
            
            -- Current audit date
            '{{ var("covid_audit_end_date", "2025-06-30") }}'::DATE AS audit_end_date
            
    {%- elif campaign_id == 'covid_2025_spring' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            'Spring 2025 COVID Vaccination Campaign' AS campaign_name,
            'covid_2024_25' AS campaign_year,
            'spring' AS campaign_period,
            
            -- Core campaign dates
            '2025-04-01'::DATE AS campaign_start_date,
            '2025-06-30'::DATE AS campaign_end_date,
            '2025-06-30'::DATE AS campaign_reference_date,
            
            -- Medication lookback dates (from START_DAT)
            '2024-10-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2024-04-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start
            '2023-04-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start  
            '2022-04-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start
            
            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2023-04-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before spring start
            '2025-03-31'::DATE AS asthma_steroid_window_1_end,          -- Up to spring 2025
            '2023-10-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from autumn 2023
            '2025-09-30'::DATE AS asthma_steroid_window_2_end,          -- Up to autumn 2025
            '2024-01-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2025-12-31'::DATE AS asthma_steroid_window_3_end,          -- Extended window
            
            -- Vaccination tracking dates
            '2025-04-01'::DATE AS vaccination_tracking_start,
            '2025-06-30'::DATE AS vaccination_tracking_end,
            '2025-03-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2025-06-30'::DATE AS decline_tracking_end,                 -- Through spring period
            
            -- Pregnancy tracking (campaign-specific)
            '2024-08-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign  
            '2025-04-01'::DATE AS pregnancy_current_start,              -- Campaign period start
            '2025-06-30'::DATE AS pregnancy_current_end,                -- Campaign period end
            '2025-01-14'::DATE AS gestational_diabetes_start,          -- Gestational diabetes tracking
            
            
            -- Individual condition eligibility flags (2024/25 campaigns)
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,  
            TRUE AS eligible_care_home,
            TRUE AS eligible_asthma,
            TRUE AS eligible_chronic_heart_disease,
            TRUE AS eligible_chronic_kidney_disease,
            TRUE AS eligible_diabetes,
            TRUE AS eligible_chronic_liver_disease,
            TRUE AS eligible_chronic_neurological_disease,
            TRUE AS eligible_chronic_respiratory_disease,
            TRUE AS eligible_morbid_obesity,
            TRUE AS eligible_asplenia,
            TRUE AS eligible_learning_disability,
            TRUE AS eligible_severe_mental_illness,
            TRUE AS eligible_pregnancy,
            TRUE AS eligible_gestational_diabetes,
            TRUE AS eligible_homeless,
            
            -- Current audit date
            '{{ var("covid_audit_end_date", "2025-06-30") }}'::DATE AS audit_end_date
            
    {%- elif campaign_id == 'covid_2025_autumn' -%}
        SELECT 
            '{{ campaign_id }}' AS campaign_id,
            'Autumn 2025 COVID Vaccination Campaign' AS campaign_name,
            'covid_2025_26' AS campaign_year,
            'autumn' AS campaign_period,
            
            -- Core campaign dates
            '2025-09-01'::DATE AS campaign_start_date,
            '2026-03-31'::DATE AS campaign_end_date,
            '2026-03-31'::DATE AS campaign_reference_date,
            
            -- Medication lookback dates (from START_DAT)
            '2025-03-01'::DATE AS immuno_medication_lookback_date,      -- 6 months before start
            '2024-09-01'::DATE AS asthma_medication_lookback_date,      -- 1 year before start
            '2023-09-01'::DATE AS asthma_admission_lookback_date,       -- 2 years before start
            '2022-09-01'::DATE AS immuno_admin_lookback_date,           -- 3 years before start
            
            -- Asthma oral steroid windows (3 overlapping 2-year periods)
            '2023-09-01'::DATE AS asthma_steroid_window_1_start,        -- 2 years before autumn start
            '2025-08-31'::DATE AS asthma_steroid_window_1_end,          -- Up to autumn 2025
            '2024-04-01'::DATE AS asthma_steroid_window_2_start,        -- 2 years from spring 2024
            '2026-03-31'::DATE AS asthma_steroid_window_2_end,          -- Up to spring 2026
            '2024-07-01'::DATE AS asthma_steroid_window_3_start,        -- Additional window
            '2026-06-30'::DATE AS asthma_steroid_window_3_end,          -- Up to spring 2026 end
            
            -- Vaccination tracking dates
            '2025-09-01'::DATE AS vaccination_tracking_start,
            '2026-03-31'::DATE AS vaccination_tracking_end,
            '2025-08-01'::DATE AS decline_tracking_start,               -- 1 month before campaign
            '2026-06-30'::DATE AS decline_tracking_end,                 -- Through spring period
            
            -- Pregnancy tracking (campaign-specific)
            '2025-01-01'::DATE AS pregnancy_lookback_start,             -- 8 months before campaign
            '2025-09-01'::DATE AS pregnancy_current_start,              -- Campaign period start  
            '2026-06-30'::DATE AS pregnancy_current_end,                -- Through spring period
            '2025-01-14'::DATE AS gestational_diabetes_start,          -- Gestational diabetes tracking
            
            -- Individual condition eligibility flags (2025/26 restricted campaigns)
            TRUE AS eligible_age_75_plus,
            TRUE AS eligible_immunosuppression,  
            TRUE AS eligible_care_home,
            TRUE AS eligible_asthma,
            FALSE AS eligible_chronic_heart_disease,       -- Removed in 2025/26
            FALSE AS eligible_chronic_kidney_disease,      -- Removed in 2025/26
            FALSE AS eligible_diabetes,                    -- Removed in 2025/26
            FALSE AS eligible_chronic_liver_disease,       -- Removed in 2025/26
            FALSE AS eligible_chronic_neurological_disease, -- Removed in 2025/26
            FALSE AS eligible_chronic_respiratory_disease, -- Removed in 2025/26
            FALSE AS eligible_morbid_obesity,              -- Removed in 2025/26
            FALSE AS eligible_asplenia,                    -- Removed in 2025/26
            TRUE AS eligible_learning_disability,
            TRUE AS eligible_severe_mental_illness,
            TRUE AS eligible_pregnancy,
            FALSE AS eligible_gestational_diabetes,        -- Removed in 2025/26
            FALSE AS eligible_homeless,                    -- Removed in 2025/26
            
            -- Current audit date
            '{{ var("covid_audit_end_date", "2025-06-30") }}'::DATE AS audit_end_date
            
    {%- else -%}
        -- Default to current campaign if unknown campaign_id
        {{ covid_campaign_config('covid_2025_autumn') }}
    {%- endif -%}
{% endmacro %}

/*
Helper macro to get a specific campaign date
Usage: {{ covid_get_campaign_date('campaign_reference_date') }}
*/
{% macro covid_get_campaign_date(date_name, campaign_id=none) %}
    {%- set campaign_id = campaign_id or var('covid_current_campaign', 'covid_2025_autumn') -%}
    (SELECT {{ date_name }} FROM ({{ covid_campaign_config(campaign_id) }}))
{% endmacro %}