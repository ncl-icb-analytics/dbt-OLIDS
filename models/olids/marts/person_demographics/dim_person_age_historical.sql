{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'age', 'historical', 'scd2'],
        cluster_by=['person_id', 'effective_start_date'],
        post_hook=[
            "COMMENT ON COLUMN {{ this }}.person_id IS 'Core: Unique person identifier across all periods'",
            "COMMENT ON COLUMN {{ this }}.sk_patient_id IS 'Core: Surrogate key for patient record'",
            "COMMENT ON COLUMN {{ this }}.effective_start_date IS 'SCD2: Period start date for temporal tracking'",
            "COMMENT ON COLUMN {{ this }}.effective_end_date IS 'SCD2: Period end date for temporal tracking (NULL = current period)'",
            "COMMENT ON COLUMN {{ this }}.period_sequence IS 'SCD2: Sequential number for each temporal period per person'",
            "COMMENT ON COLUMN {{ this }}.birth_year IS 'Static: Birth year (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.birth_month IS 'Static: Birth month (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.birth_date_approx IS 'Static: Approximate birth date (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.death_year IS 'Static: Death year if deceased (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.death_month IS 'Static: Death month if deceased (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.death_date_approx IS 'Static: Approximate death date if deceased (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.is_deceased IS 'Static: Death status flag (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.age IS 'SCD2: Age calculated as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_months IS 'SCD2: Age in months as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_weeks_approx IS 'SCD2: Age in weeks as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_days_approx IS 'SCD2: Age in days as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_band_5y IS 'SCD2: 5-year age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_band_10y IS 'SCD2: 10-year age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_band_nhs IS 'SCD2: NHS age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_band_ons IS 'SCD2: ONS age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_life_stage IS 'SCD2: Life stage as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.is_current_period IS 'SCD2: Flag indicating if this is the current active period'"
        ])
}}

/*
Historical Person Age Dimension Table (Type 2 SCD)

Calculates age and age bands using modern Type 2 slowly changing dimensions approach.
Much more efficient than person-month grain - only creates rows when age bands actually change.

Key Features:

• Type 2 SCD with effective_start_date and effective_end_date (NULL = current period)

• 5-year rolling history window for performance optimisation

• Modern SCD2 implementation without sentinel dates (no 9999-12-31 values)

• Age milestones trigger new periods (every 5 years for age band changes)

• Efficient storage - periods created only when age bands change, not monthly

• Point-in-time lookups using proper temporal logic

Implementation Notes:

• Uses NULL effective_end_date for current periods (not sentinel dates)

• Limited to last 5 years of data to balance completeness with performance

• Change points triggered by: age milestones at 5-year intervals (0, 5, 10, 15, etc.)

• Age calculations use period start date for temporal accuracy

For analysis queries, join using:
WHERE analysis_date >= effective_start_date 
  AND (effective_end_date IS NULL OR analysis_date < effective_end_date)
*/

WITH age_milestones AS (
    -- Calculate key age milestone dates when age bands change
    SELECT 
        person_id,
        birth_date_approx,
        death_date_approx,
        is_deceased,
        sk_patient_id,
        birth_year,
        birth_month,
        death_year,
        death_month,
        -- Generate milestone dates for 5-year age band changes
        DATEADD('year', milestone_age, birth_date_approx) as milestone_date,
        milestone_age
    FROM {{ ref('dim_person_birth_death') }}
    CROSS JOIN (
        SELECT column1 as milestone_age 
        FROM VALUES (0),(5),(10),(15),(20),(25),(30),(35),(40),(45),(50),(55),(60),(65),(70),(75),(80),(85),(90),(95),(100)
    ) ages
    WHERE DATEADD('year', milestone_age, birth_date_approx) <= COALESCE(death_date_approx, CURRENT_DATE)
        AND DATEADD('year', milestone_age, birth_date_approx) >= DATE_TRUNC('month', DATEADD('year', -5, CURRENT_DATE)) -- Last 5 years only
),

temporal_periods AS (
    -- Create temporal periods between age milestones
    SELECT 
        person_id,
        birth_date_approx,
        death_date_approx,
        is_deceased,
        sk_patient_id,
        birth_year,
        birth_month,
        death_year,
        death_month,
        milestone_date as effective_start_date,
        LEAD(milestone_date) OVER (
            PARTITION BY person_id 
            ORDER BY milestone_date
        ) as effective_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY milestone_date
        ) as period_sequence
    FROM age_milestones
)

SELECT
    -- Core identifiers and temporal boundaries
    person_id,
    sk_patient_id,
    effective_start_date,
    effective_end_date,
    period_sequence,
    
    -- Birth and death information
    birth_year,
    birth_month,
    birth_date_approx,
    death_year,
    death_month,
    death_date_approx,
    is_deceased,
    
    -- Age calculations (as of period start)
    FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) AS age,
    DATEDIFF(month, birth_date_approx, effective_start_date) AS age_months,
    FLOOR(DATEDIFF(day, birth_date_approx, effective_start_date) / 7) AS age_weeks_approx,
    DATEDIFF(day, birth_date_approx, effective_start_date) AS age_days_approx,

    -- 5-year age bands (calculated from period start age)
    CASE
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 0 THEN 'Unknown'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) >= 100 THEN '100+'
        ELSE TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) / 5) * 5) || '-' || 
             TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) / 5) * 5 + 4)
    END AS age_band_5y,

    -- 10-year age bands
    CASE
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 0 THEN 'Unknown'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) >= 100 THEN '100+'
        ELSE TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) / 10) * 10) || '-' || 
             TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) / 10) * 10 + 9)
    END AS age_band_10y,

    -- NHS Digital age bands
    CASE
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 0 THEN 'Unknown'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 5 THEN '0-4'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 15 THEN '5-14'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 25 THEN '15-24'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 35 THEN '25-34'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 45 THEN '35-44'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 55 THEN '45-54'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 65 THEN '55-64'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 75 THEN '65-74'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 85 THEN '75-84'
        ELSE '85+'
    END AS age_band_nhs,

    -- ONS age bands
    CASE
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 0 THEN 'Unknown'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 5 THEN '0-4'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 10 THEN '5-9'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 15 THEN '10-14'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 20 THEN '15-19'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 25 THEN '20-24'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 30 THEN '25-29'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 35 THEN '30-34'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 40 THEN '35-39'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 45 THEN '40-44'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 50 THEN '45-49'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 55 THEN '50-54'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 60 THEN '55-59'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 65 THEN '60-64'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 70 THEN '65-69'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 75 THEN '70-74'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 80 THEN '75-79'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 85 THEN '80-84'
        ELSE '85+'
    END AS age_band_ons,

    -- Life stage categories
    CASE
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 0 THEN 'Unknown'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 1 THEN 'Infant'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 4 THEN 'Toddler'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 13 THEN 'Child'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 20 THEN 'Adolescent'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 25 THEN 'Young Adult'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 60 THEN 'Adult'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 75 THEN 'Older Adult'
        WHEN FLOOR(DATEDIFF(month, birth_date_approx, effective_start_date) / 12) < 85 THEN 'Elderly'
        ELSE 'Very Elderly'
    END AS age_life_stage,
    
    -- Efficiency flags
    CASE WHEN effective_end_date IS NULL THEN TRUE ELSE FALSE END as is_current_period

FROM temporal_periods
ORDER BY person_id, effective_start_date