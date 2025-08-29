/*
Flu Vaccination Weekly Uptake by Practice and Risk Group

This model provides weekly vaccination tracking from September to March
at practice and risk group level for time series analysis.

Key features:
- Weekly granularity from campaign start (September) to end (March)
- One row per week, practice, and risk group
- Includes eligible population and vaccination counts
- Cumulative and weekly vaccination metrics
- Point-in-time registration status
- Enables proper time series analysis and forecasting

Usage:
- Filter by risk_group to focus on specific eligibility criteria
- Aggregate to PCN or borough level for organisational analysis
- Track progress against targets by risk groups
- Build time series dashboards
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'week_number', 'practice_code', 'risk_group']
) }}

WITH campaign_configs AS (
    -- Get all campaign configurations
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Generate week spine for each campaign (September to March)
week_spine AS (
    SELECT 
        cc.campaign_id,
        cc.campaign_name,
        cc.campaign_start_date,
        cc.campaign_end_date,
        cc.campaign_reference_date,
        seq.week_number,
        DATEADD('week', seq.week_number - 1, DATE_TRUNC('week', cc.campaign_start_date)) AS week_start_date,
        DATEADD('day', 6, DATEADD('week', seq.week_number - 1, DATE_TRUNC('week', cc.campaign_start_date))) AS week_end_date
    FROM campaign_configs cc
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS week_number
        FROM TABLE(GENERATOR(ROWCOUNT => 30))  -- ~30 weeks from Sept to March
    ) seq
    WHERE seq.week_number <= DATEDIFF('week', cc.campaign_start_date, cc.campaign_end_date) + 1
),

-- Get person registration periods for point-in-time is_active
person_registrations AS (
    SELECT 
        person_id,
        practice_ods_code as practice_code,
        registration_start_date,
        registration_end_date
    FROM {{ ref('int_patient_registrations') }}
),

-- Get vaccination activity with point-in-time status
vaccination_activity AS (
    SELECT 
        u.campaign_id,
        u.practice_code,
        u.practice_borough,
        u.risk_group,
        u.person_id,
        u.vaccination_date,
        u.vaccinated,
        u.declined,
        u.eligible_no_record,
        u.is_eligible,
        -- Registration periods for point-in-time checks
        pr.registration_start_date,
        pr.registration_end_date
    FROM {{ ref('fct_flu_uptake') }} u
    LEFT JOIN person_registrations pr
        ON u.person_id = pr.person_id
        AND u.practice_code = pr.practice_code
    WHERE u.practice_code IS NOT NULL
),

-- Get eligible population by practice and risk group
eligible_by_practice_risk_group AS (
    SELECT 
        campaign_id,
        practice_code,
        risk_group,
        COUNT(DISTINCT CASE WHEN is_eligible THEN person_id END) AS eligible_count,
        COUNT(DISTINCT person_id) AS total_population
    FROM vaccination_activity
    WHERE registration_start_date IS NOT NULL  -- Only include people with known registration periods
    GROUP BY 1, 2, 3
),

-- Calculate weekly vaccinations by practice and risk group
weekly_vaccinations AS (
    SELECT 
        v.campaign_id,
        v.practice_code,
        v.risk_group,
        ws.week_number,
        COUNT(DISTINCT CASE WHEN v.vaccinated AND v.vaccination_date BETWEEN ws.week_start_date AND ws.week_end_date 
                           THEN v.person_id END) AS weekly_vaccinated,
        COUNT(DISTINCT CASE WHEN v.declined AND v.vaccination_date BETWEEN ws.week_start_date AND ws.week_end_date 
                           THEN v.person_id END) AS weekly_declined
    FROM vaccination_activity v
    CROSS JOIN week_spine ws
    WHERE v.campaign_id = ws.campaign_id
        -- Only include if person was registered at the practice during this week
        AND v.registration_start_date <= ws.week_end_date
        AND (v.registration_end_date IS NULL OR v.registration_end_date >= ws.week_start_date)
    GROUP BY 1, 2, 3, 4
),

-- Combine all data
final_weekly AS (
    SELECT 
        ws.campaign_id,
        ws.campaign_name,
        ws.week_number,
        ws.week_start_date,
        ws.week_end_date,
        
        -- Practice information
        ep.practice_code,
        p.practice_name,
        p.pcn_code,
        p.pcn_name,
        p.practice_borough,
        p.practice_neighbourhood,
        
        -- Risk group
        ep.risk_group,
        
        -- Eligible population (constant across weeks)
        ep.eligible_count,
        ep.total_population,
        
        -- Weekly activity
        COALESCE(wv.weekly_vaccinated, 0) AS weekly_vaccinated,
        COALESCE(wv.weekly_declined, 0) AS weekly_declined,
        
        -- Calculate cumulative counts
        SUM(COALESCE(wv.weekly_vaccinated, 0)) OVER (
            PARTITION BY ws.campaign_id, ep.practice_code, ep.risk_group
            ORDER BY ws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_vaccinated,
        
        SUM(COALESCE(wv.weekly_declined, 0)) OVER (
            PARTITION BY ws.campaign_id, ep.practice_code, ep.risk_group
            ORDER BY ws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_declined,
        
        -- Calculate rates
        ROUND(100.0 * SUM(COALESCE(wv.weekly_vaccinated, 0)) OVER (
            PARTITION BY ws.campaign_id, ep.practice_code, ep.risk_group
            ORDER BY ws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / NULLIF(ep.eligible_count, 0), 1) AS uptake_rate_percent,
        
        -- Remaining eligible
        ep.eligible_count - SUM(COALESCE(wv.weekly_vaccinated, 0)) OVER (
            PARTITION BY ws.campaign_id, ep.practice_code, ep.risk_group
            ORDER BY ws.week_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS remaining_eligible,
        
        -- Week metadata
        CASE 
            WHEN ws.week_number <= 4 THEN 'Early Campaign'
            WHEN ws.week_number BETWEEN 5 AND 12 THEN 'Peak Period'
            WHEN ws.week_number BETWEEN 13 AND 20 THEN 'Late Campaign'
            ELSE 'End Period'
        END AS campaign_phase,
        
        -- Is this current data?
        CASE 
            WHEN ws.week_end_date <= CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_historical_week,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM week_spine ws
    CROSS JOIN eligible_by_practice_risk_group ep
    LEFT JOIN weekly_vaccinations wv
        ON ws.campaign_id = wv.campaign_id
        AND ws.week_number = wv.week_number
        AND ep.practice_code = wv.practice_code
        AND ep.risk_group = wv.risk_group
    LEFT JOIN (
        SELECT DISTINCT 
            practice_code,
            practice_name,
            pcn_code,
            pcn_name,
            practice_borough,
            practice_neighbourhood
        FROM {{ ref('dim_person_demographics') }}
        WHERE practice_code IS NOT NULL
    ) p ON ep.practice_code = p.practice_code
    WHERE ep.campaign_id = ws.campaign_id
)

SELECT * FROM final_weekly
ORDER BY 
    campaign_id DESC,
    week_number,
    practice_code,
    -- Order risk groups logically: Age-based first, then clinical conditions
    CASE 
        WHEN risk_group = 'Age 65 and Over' THEN 1
        WHEN risk_group LIKE '%Children%' THEN 2
        WHEN risk_group = 'Pregnancy' THEN 3
        WHEN risk_group = 'Immunosuppression' THEN 4
        WHEN risk_group = 'Long-term Residential Care' THEN 5
        WHEN risk_group = 'Diabetes' THEN 6
        WHEN risk_group = 'Chronic Kidney Disease' THEN 7
        WHEN risk_group = 'Chronic Heart Disease' THEN 8
        WHEN risk_group = 'Chronic Respiratory Disease' THEN 9
        WHEN risk_group LIKE '%Asthma%' THEN 10
        WHEN risk_group = 'Chronic Liver Disease' THEN 11
        WHEN risk_group = 'Chronic Neurological Disease' THEN 12
        WHEN risk_group = 'Learning Disability' THEN 13
        WHEN risk_group = 'Severe Obesity' THEN 14
        WHEN risk_group = 'Asplenia' THEN 15
        WHEN risk_group = 'Health & Social Care Worker' THEN 16
        WHEN risk_group = 'Unpaid Carer' THEN 17
        WHEN risk_group = 'Household Immunocompromised Contact' THEN 18
        WHEN risk_group = 'Homeless' THEN 19
        ELSE 99
    END,
    risk_group