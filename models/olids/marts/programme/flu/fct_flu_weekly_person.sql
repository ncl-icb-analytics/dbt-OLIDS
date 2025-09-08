/*
Flu Weekly Person-Level Timeseries

Expands the sparse person-week snapshot to provide complete weekly status
for every eligible person, supporting complex multi-dimensional filtering in PowerBI.

Key features:
- Complete person × week × campaign grain
- All demographic and risk group dimensions included
- Vaccination status carried forward using window functions
- Optimised for PowerBI filtering by any combination of attributes
- Supports complex queries like "diabetic AND over 65 AND high deprivation"

This table enables rich analytical capability while being efficient through
the sparse source and targeted materialisation.
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'week_number', 'practice_code'],
    pre_hook="DROP TABLE IF EXISTS {{ this }}_temp"
) }}

WITH campaign_configs AS (
    -- Get all campaign configurations
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

effective_dates AS (
    -- Determine effective end date for each campaign
    SELECT 
        campaign_id,
        campaign_name,
        campaign_start_date,
        CASE 
            WHEN CURRENT_DATE > campaign_reference_date THEN campaign_reference_date
            WHEN CURRENT_DATE > campaign_end_date THEN campaign_end_date
            ELSE CURRENT_DATE
        END AS effective_end_date,
        DATEDIFF('week', campaign_start_date, 
            LEAST(
                CASE 
                    WHEN CURRENT_DATE > campaign_reference_date THEN campaign_reference_date
                    WHEN CURRENT_DATE > campaign_end_date THEN campaign_end_date
                    ELSE CURRENT_DATE
                END,
                campaign_reference_date
            )
        ) + 1 AS max_week_number
    FROM campaign_configs
),

-- Generate complete week spine for each eligible person
person_week_spine AS (
    SELECT 
        e.campaign_id,
        e.person_id,
        seq.week_number,
        DATEADD('week', seq.week_number - 1, DATE_TRUNC('week', ed.campaign_start_date)) AS week_start_date,
        ed.campaign_name,
        ed.max_week_number
    FROM {{ ref('fct_flu_eligibility') }} e
    JOIN effective_dates ed ON e.campaign_id = ed.campaign_id
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS week_number
        FROM TABLE(GENERATOR(ROWCOUNT => 26))
    ) seq
    WHERE seq.week_number <= ed.max_week_number
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.campaign_id, e.person_id ORDER BY e.eligibility_priority) = 1
),

-- Get sparse snapshot data
sparse_data AS (
    SELECT 
        campaign_id,
        person_id,
        week_number,
        vaccination_status,
        is_vaccinated,
        primary_risk_group
    FROM {{ ref('int_flu_person_week_snapshot') }}
),

-- Expand sparse data to complete timeline
expanded_timeline AS (
    SELECT 
        pws.campaign_id,
        pws.campaign_name,
        pws.person_id,
        pws.week_number,
        pws.week_start_date,
        
        -- Carry forward vaccination status using window function
        LAST_VALUE(sd.vaccination_status IGNORE NULLS) OVER (
            PARTITION BY pws.campaign_id, pws.person_id
            ORDER BY pws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS current_vaccination_status,
        
        -- Carry forward vaccination flag
        LAST_VALUE(sd.is_vaccinated IGNORE NULLS) OVER (
            PARTITION BY pws.campaign_id, pws.person_id
            ORDER BY pws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS is_currently_vaccinated,
        
        -- Determine if this was the week of vaccination
        CASE 
            WHEN sd.is_vaccinated = TRUE AND sd.week_number = pws.week_number 
            THEN TRUE ELSE FALSE 
        END AS vaccination_occurred_this_week,
        
        -- Get primary risk group from eligibility (filled later)
        sd.primary_risk_group
        
    FROM person_week_spine pws
    LEFT JOIN sparse_data sd
        ON pws.campaign_id = sd.campaign_id
        AND pws.person_id = sd.person_id
        AND pws.week_number = sd.week_number
),

-- Add comprehensive demographics and risk groups
final_with_demographics AS (
    SELECT 
        et.campaign_id,
        et.campaign_name,
        et.person_id,
        et.week_number,
        et.week_start_date,
        
        -- Vaccination status
        COALESCE(et.current_vaccination_status, 'ELIGIBLE_NOT_VACCINATED') AS vaccination_status,
        COALESCE(et.is_currently_vaccinated, FALSE) AS is_vaccinated,
        et.vaccination_occurred_this_week,
        
        -- Fill primary risk group from eligibility
        COALESCE(
            et.primary_risk_group,
            ef.risk_group,
            'Unknown'
        ) AS primary_risk_group,
        
        -- Demographics (current snapshot for efficiency)
        d.sex,
        d.age,
        d.age_band_5y,
        d.age_band_10y,
        d.age_band_nhs,
        d.age_life_stage,
        d.ethnicity_category,
        d.ethnicity_subcategory,
        d.main_language,
        d.interpreter_needed,
        
        -- Geography and organisation
        d.practice_code,
        d.practice_name,
        d.pcn_code,
        d.pcn_name,
        d.practice_borough,
        d.practice_neighbourhood,
        d.lsoa_code_21,
        d.ward_name,
        d.imd_decile_19,
        d.imd_quintile_19,
        
        -- Risk group flags (for complex filtering)
        CASE WHEN ef.campaign_category = 'Age Based' THEN TRUE ELSE FALSE END AS is_age_eligible,
        CASE WHEN ef.campaign_category = 'Clinical Condition' THEN TRUE ELSE FALSE END AS is_clinical_eligible,
        CASE WHEN ef.campaign_category = 'At Risk Group' THEN TRUE ELSE FALSE END AS is_at_risk_eligible,
        
        -- Individual risk group flags for filtering
        CASE WHEN ef.risk_group = 'Over 65' THEN TRUE ELSE FALSE END AS is_over_65,
        CASE WHEN ef.risk_group = 'Children Preschool' THEN TRUE ELSE FALSE END AS is_preschool,
        CASE WHEN ef.risk_group = 'Children School Age' THEN TRUE ELSE FALSE END AS is_school_age,
        CASE WHEN ef.risk_group = 'Diabetes' THEN TRUE ELSE FALSE END AS has_diabetes,
        CASE WHEN ef.risk_group = 'Chronic Heart Disease' THEN TRUE ELSE FALSE END AS has_heart_disease,
        CASE WHEN ef.risk_group = 'Chronic Respiratory Disease' THEN TRUE ELSE FALSE END AS has_respiratory_disease,
        CASE WHEN ef.risk_group = 'Chronic Kidney Disease' THEN TRUE ELSE FALSE END AS has_kidney_disease,
        CASE WHEN ef.risk_group = 'Chronic Liver Disease' THEN TRUE ELSE FALSE END AS has_liver_disease,
        CASE WHEN ef.risk_group = 'Chronic Neurological Disease' THEN TRUE ELSE FALSE END AS has_neurological_disease,
        CASE WHEN ef.risk_group = 'Immunosuppression' THEN TRUE ELSE FALSE END AS has_immunosuppression,
        CASE WHEN ef.risk_group = 'Asplenia' THEN TRUE ELSE FALSE END AS has_asplenia,
        CASE WHEN ef.risk_group = 'Pregnancy' THEN TRUE ELSE FALSE END AS is_pregnant,
        CASE WHEN ef.risk_group = 'Severe Obesity' THEN TRUE ELSE FALSE END AS has_severe_obesity,
        CASE WHEN ef.risk_group = 'Learning Disability' THEN TRUE ELSE FALSE END AS has_learning_disability,
        CASE WHEN ef.risk_group = 'Carer' THEN TRUE ELSE FALSE END AS is_carer,
        CASE WHEN ef.risk_group = 'Health Social Care Worker' THEN TRUE ELSE FALSE END AS is_health_worker,
        CASE WHEN ef.risk_group = 'Long Term Residential Care' THEN TRUE ELSE FALSE END AS in_residential_care,
        CASE WHEN ef.risk_group = 'Homeless' THEN TRUE ELSE FALSE END AS is_homeless,
        CASE WHEN ef.risk_group = 'Household Immunocompromised' THEN TRUE ELSE FALSE END AS household_immunocompromised,
        
        -- Weekly position metrics
        DATEDIFF('day', et.week_start_date, CURRENT_DATE) AS days_since_week_start,
        CASE 
            WHEN et.week_number BETWEEN 1 AND 4 THEN 'Early Campaign'
            WHEN et.week_number BETWEEN 5 AND 12 THEN 'Peak Campaign'
            WHEN et.week_number BETWEEN 13 AND 20 THEN 'Late Campaign'
            ELSE 'End Campaign'
        END AS campaign_phase,
        
        -- Is this the current/latest week with data?
        CASE 
            WHEN et.week_number = MAX(et.week_number) OVER (PARTITION BY et.campaign_id)
            THEN TRUE ELSE FALSE 
        END AS is_latest_week,
        
        CURRENT_TIMESTAMP AS created_at
        
    FROM expanded_timeline et
    -- Join demographics once
    JOIN {{ ref('dim_person_demographics') }} d 
        ON et.person_id = d.person_id
    -- Join eligibility for risk group flags
    LEFT JOIN {{ ref('fct_flu_eligibility') }} ef
        ON et.campaign_id = ef.campaign_id
        AND et.person_id = ef.person_id
        AND et.primary_risk_group = ef.risk_group
    
    WHERE d.age BETWEEN 0 AND 120  -- Filter out invalid ages
    
    -- Deduplicate in case of multiple eligibility records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY et.campaign_id, et.person_id, et.week_number 
        ORDER BY ef.eligibility_priority NULLS LAST
    ) = 1
)

SELECT * FROM final_with_demographics
ORDER BY campaign_id DESC, week_number, practice_code, person_id