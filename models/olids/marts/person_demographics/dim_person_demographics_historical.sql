{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'historical', 'scd2'],
        cluster_by=['person_id', 'effective_start_date'],
        post_hook=[
            "COMMENT ON COLUMN {{ this }}.person_id IS 'Core: Unique person identifier across all periods'",
            "COMMENT ON COLUMN {{ this }}.sk_patient_id IS 'Core: Surrogate key for patient record'",
            "COMMENT ON COLUMN {{ this }}.effective_start_date IS 'SCD2: Period start date for temporal tracking'",
            "COMMENT ON COLUMN {{ this }}.effective_end_date IS 'SCD2: Period end date for temporal tracking (NULL = current period)'",
            "COMMENT ON COLUMN {{ this }}.period_sequence IS 'SCD2: Sequential number for each temporal period per person'",
            "COMMENT ON COLUMN {{ this }}.age IS 'SCD2: Age calculated as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.birth_year IS 'Static: Birth year (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.birth_date_approx IS 'Static: Approximate birth date (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.death_year IS 'Static: Death year if deceased (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.death_date_approx IS 'Static: Approximate death date if deceased (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.is_deceased IS 'Static: Death status flag (stable demographic)'",
            "COMMENT ON COLUMN {{ this }}.age_band_5y IS 'SCD2: 5-year age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_band_10y IS 'SCD2: 10-year age band as of effective_start_date (temporal)'", 
            "COMMENT ON COLUMN {{ this }}.age_band_nhs IS 'SCD2: NHS age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_band_ons IS 'SCD2: ONS age band as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.age_life_stage IS 'SCD2: Life stage as of effective_start_date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.practice_code IS 'SCD2: Practice registration valid during this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.practice_name IS 'SCD2: Practice name valid during this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.registration_start_date IS 'SCD2: Practice registration start date (temporal)'",
            "COMMENT ON COLUMN {{ this }}.sex IS 'Static: Sex assumed stable over time (not temporally tracked)'",
            "COMMENT ON COLUMN {{ this }}.ethnicity_category IS 'SCD2: Ethnicity category valid during this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.ethnicity_subcategory IS 'SCD2: Ethnicity subcategory valid during this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.ethnicity_granular IS 'SCD2: Detailed ethnicity valid during this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.ethnicity_category_sort IS 'SCD2: Ethnicity category sort order for this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.ethnicity_display_sort_key IS 'SCD2: Ethnicity display sort key for this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.main_language IS 'Static: Main language assumed stable over time (not temporally tracked)'",
            "COMMENT ON COLUMN {{ this }}.language_type IS 'Static: Language type assumed stable over time (not temporally tracked)'",
            "COMMENT ON COLUMN {{ this }}.interpreter_type IS 'Static: Interpreter type assumed stable over time (not temporally tracked)'",
            "COMMENT ON COLUMN {{ this }}.interpreter_needed IS 'Static: Interpreter needed flag assumed stable over time (not temporally tracked)'",
            "COMMENT ON COLUMN {{ this }}.pcn_code IS 'Linked: PCN code linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.pcn_name IS 'Linked: PCN name linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.pcn_name_with_borough IS 'Linked: PCN name with borough linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_borough IS 'Linked: Practice borough linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_postcode IS 'Linked: Practice postcode linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_lsoa IS 'Linked: Practice LSOA linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_msoa IS 'Linked: Practice MSOA linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_latitude IS 'Linked: Practice latitude linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_longitude IS 'Linked: Practice longitude linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.practice_neighbourhood IS 'Linked: Practice neighbourhood linked to temporal practice registration'",
            "COMMENT ON COLUMN {{ this }}.post_code_hash IS 'SCD2: Address postcode hash valid during this period (temporal)'",
            "COMMENT ON COLUMN {{ this }}.uprn_hash IS 'Placeholder: UPRN hash for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.household_id IS 'Placeholder: Household identifier for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.lsoa_code_21 IS 'Placeholder: LSOA 2021 code for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.lsoa_name_21 IS 'Placeholder: LSOA 2021 name for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.ward_code IS 'Placeholder: Ward code for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.ward_name IS 'Placeholder: Ward name for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.imd_decile_19 IS 'Placeholder: IMD 2019 decile for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.imd_quintile_19 IS 'Placeholder: IMD 2019 quintile for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.patient_neighbourhood IS 'Placeholder: Patient neighbourhood for future implementation (not yet available)'",
            "COMMENT ON COLUMN {{ this }}.is_current_period IS 'SCD2: Flag indicating if this is the current active period'",
            "COMMENT ON COLUMN {{ this }}.age_changes_in_period IS 'SCD2: Flag indicating if age bands change within this period'"
        ])
}}

/*
Historical Person Demographics Dimension Table (Type 2 SCD)

Provides person demographics with temporal context using modern Type 2 slowly changing dimensions approach.
Much more efficient than person-month grain - only creates new rows when demographics actually change.

Key Features:

• Type 2 SCD with effective_start_date and effective_end_date (NULL = current period)

• 5-year rolling history window for performance optimisation

• Modern SCD2 implementation without sentinel dates (no 9999-12-31 values)

• Temporal tracking for: age progression, practice registration, address changes, ethnicity updates

• Static tracking for: sex, language, interpreter needs (assumed stable)

• Efficient storage - periods created only when demographics change, not monthly

• Point-in-time lookups using proper temporal logic

Implementation Notes:

• Uses NULL effective_end_date for current periods (not sentinel dates)

• Limited to last 5 years of data to balance completeness with performance

• Change points triggered by: practice moves, address changes, age milestones (5-year bands), ethnicity recordings

• Ethnicity reflects clinical observations over time, not assumed static

Data Quality Filters:

• Excludes persons without practice registration (orphaned patients)

• Excludes persons without valid birth dates (required for age calculations)

For analysis queries, join using:
WHERE analysis_date >= effective_start_date 
  AND (effective_end_date IS NULL OR analysis_date < effective_end_date)
*/

WITH practice_changes AS (
    -- Get all practice registration changes as temporal boundaries
    SELECT 
        person_id,
        practice_code,
        practice_name,
        registration_start_date,
        registration_end_date,
        is_current_registration,
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY registration_start_date, registration_end_date NULLS LAST
        ) as practice_sequence
    FROM {{ ref('dim_person_historical_practice') }}
),

age_milestones AS (
    -- Calculate key age milestone dates for each person (when age bands change)
    SELECT 
        person_id,
        birth_date_approx,
        death_date_approx,
        -- Generate age milestone dates (every 5 years for age bands)
        DATEADD('year', milestone_age, birth_date_approx) as milestone_date,
        milestone_age
    FROM {{ ref('dim_person_birth_death') }}
    CROSS JOIN (
        SELECT column1 as milestone_age 
        FROM VALUES (0),(5),(10),(15),(20),(25),(30),(35),(40),(45),(50),(55),(60),(65),(70),(75),(80),(85),(90),(95),(100)
    ) ages
    WHERE DATEADD('year', milestone_age, birth_date_approx) <= COALESCE(death_date_approx, CURRENT_DATE)
),

address_changes AS (
    -- Get all address changes as temporal boundaries
    SELECT DISTINCT
        ipr.person_id,
        pa.start_date as address_start_date,
        pa.end_date as address_end_date,
        pa.post_code_hash,
        ROW_NUMBER() OVER (
            PARTITION BY ipr.person_id 
            ORDER BY pa.start_date, pa.lds_datetime_data_acquired
        ) as address_sequence
    FROM {{ ref('int_patient_registrations') }} ipr
    INNER JOIN {{ ref('stg_olids_patient_address') }} pa
        ON ipr.patient_id = pa.patient_id
    WHERE pa.start_date >= DATE_TRUNC('month', DATEADD('year', -5, CURRENT_DATE)) -- Last 5 years only
),

ethnicity_changes AS (
    -- Get all ethnicity changes as temporal boundaries
    SELECT DISTINCT
        ea.person_id,
        ea.clinical_effective_date as ethnicity_date,
        ea.ethnicity_category,
        ea.ethnicity_subcategory,
        ea.ethnicity_granular,
        ea.category_sort,
        ea.display_sort_key,
        ROW_NUMBER() OVER (
            PARTITION BY ea.person_id 
            ORDER BY ea.clinical_effective_date DESC, ea.preference_rank ASC
        ) as ethnicity_sequence
    FROM {{ ref('int_ethnicity_all') }} ea
    WHERE ea.clinical_effective_date >= DATE_TRUNC('month', DATEADD('year', -5, CURRENT_DATE)) -- Last 5 years only
),

active_status_changes AS (
    -- Get all active status changes as temporal boundaries
    SELECT DISTINCT
        pas.person_id,
        COALESCE(pas.current_registration_start, CURRENT_DATE) as status_change_date,
        pas.is_active,
        pas.inactive_reason
    FROM {{ ref('dim_person_active_status') }} pas
    WHERE COALESCE(pas.current_registration_start, CURRENT_DATE) >= DATE_TRUNC('month', DATEADD('year', -5, CURRENT_DATE)) -- Last 5 years only
),

all_change_points AS (
    -- Combine all potential change points (practice + age + address + ethnicity changes)
    SELECT person_id, registration_start_date as change_date, 'practice_start' as change_type
    FROM practice_changes
    
    UNION ALL
    
    SELECT person_id, registration_end_date as change_date, 'practice_end' as change_type  
    FROM practice_changes
    WHERE registration_end_date IS NOT NULL
    
    UNION ALL
    
    SELECT person_id, milestone_date as change_date, 'age_milestone' as change_type
    FROM age_milestones
    WHERE milestone_date >= DATE_TRUNC('month', DATEADD('year', -5, CURRENT_DATE)) -- Last 5 years only
    
    UNION ALL
    
    SELECT person_id, address_start_date as change_date, 'address_start' as change_type
    FROM address_changes
    WHERE address_start_date IS NOT NULL
    
    UNION ALL
    
    SELECT person_id, address_end_date as change_date, 'address_end' as change_type
    FROM address_changes
    WHERE address_end_date IS NOT NULL
    
    UNION ALL
    
    SELECT person_id, ethnicity_date as change_date, 'ethnicity_change' as change_type
    FROM ethnicity_changes
    WHERE ethnicity_date IS NOT NULL
    
    UNION ALL
    
    SELECT person_id, status_change_date as change_date, 'active_status_change' as change_type
    FROM active_status_changes
    WHERE status_change_date IS NOT NULL
),

temporal_periods AS (
    -- Create temporal periods between change points
    SELECT 
        person_id,
        change_date as effective_start_date,
        LEAD(change_date) OVER (
            PARTITION BY person_id 
            ORDER BY change_date
        ) as effective_end_date,
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY change_date
        ) as period_sequence
    FROM (
        SELECT DISTINCT person_id, change_date
        FROM all_change_points
        WHERE change_date IS NOT NULL
    ) dedupe
),

demographics_by_period AS (
    -- Calculate demographics for each temporal period
    SELECT 
        tp.person_id,
        tp.effective_start_date,
        tp.effective_end_date,
        tp.period_sequence,
        
        -- Calculate age as of period start
        FLOOR(DATEDIFF(month, bd.birth_date_approx, tp.effective_start_date) / 12) AS age_at_period_start,
        
        -- Calculate age as of period end (for age band validation)
        FLOOR(DATEDIFF(month, bd.birth_date_approx, COALESCE(tp.effective_end_date, CURRENT_DATE)) / 12) AS age_at_period_end,
        
        -- Basic person attributes
        bd.sk_patient_id,
        bd.birth_year,
        bd.birth_month,
        bd.birth_date_approx,
        bd.death_year,  
        bd.death_date_approx,
        bd.is_deceased,
        
        -- Get practice valid during this period
        pc.practice_code,
        pc.practice_name,
        pc.registration_start_date,
        pc.is_current_registration,
        
        -- Get address valid during this period
        ac.post_code_hash,
        ac.address_start_date,
        
        -- Get ethnicity valid during this period
        ec.ethnicity_category,
        ec.ethnicity_subcategory,
        ec.ethnicity_granular,
        ec.category_sort,
        ec.display_sort_key,
        
        -- Get active status valid during this period
        asc.is_active,
        asc.inactive_reason
        
    FROM temporal_periods tp
    INNER JOIN {{ ref('dim_person_birth_death') }} bd
        ON tp.person_id = bd.person_id
    LEFT JOIN practice_changes pc
        ON tp.person_id = pc.person_id
        AND pc.registration_start_date <= tp.effective_start_date
        AND (pc.registration_end_date IS NULL OR pc.registration_end_date >= COALESCE(tp.effective_end_date, CURRENT_DATE))
    LEFT JOIN address_changes ac
        ON tp.person_id = ac.person_id
        AND ac.address_start_date <= tp.effective_start_date
        AND (ac.address_end_date IS NULL OR ac.address_end_date >= COALESCE(tp.effective_end_date, CURRENT_DATE))
    LEFT JOIN ethnicity_changes ec
        ON tp.person_id = ec.person_id
        AND ec.ethnicity_date <= tp.effective_start_date
    LEFT JOIN active_status_changes asc
        ON tp.person_id = asc.person_id
        AND asc.status_change_date <= tp.effective_start_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tp.person_id, tp.effective_start_date
        ORDER BY pc.is_current_registration DESC, pc.registration_start_date DESC,
                 ac.address_start_date DESC NULLS LAST,
                 ec.ethnicity_date DESC NULLS LAST, ec.ethnicity_sequence ASC,
                 asc.status_change_date DESC NULLS LAST
    ) = 1
)

SELECT
    -- Core identifiers and temporal boundaries
    dbp.person_id,
    dbp.sk_patient_id,
    dbp.effective_start_date,
    dbp.effective_end_date,
    ROW_NUMBER() OVER (
        PARTITION BY dbp.person_id 
        ORDER BY dbp.effective_start_date
    ) AS period_sequence,
    
    -- Age and temporal attributes
    dbp.age_at_period_start as age,
    dbp.birth_year,
    dbp.birth_date_approx,
    -- Additional birth date format for legacy compatibility
    CASE
        WHEN dbp.birth_year IS NOT NULL AND dbp.birth_month IS NOT NULL
            THEN LAST_DAY(DATE_FROM_PARTS(dbp.birth_year, dbp.birth_month, 1))
        ELSE NULL
    END AS birth_date_approx_end_of_month,
    -- Conservative age calculation using last day of birth month
    CASE
        WHEN dbp.birth_year IS NOT NULL AND dbp.birth_month IS NOT NULL THEN
            CASE
                WHEN dbp.effective_start_date >= DATEADD(
                        year,
                        DATEDIFF(year,
                                 LAST_DAY(DATE_FROM_PARTS(dbp.birth_year, dbp.birth_month, 1)),
                                 dbp.effective_start_date),
                        LAST_DAY(DATE_FROM_PARTS(dbp.birth_year, dbp.birth_month, 1))
                     )
                THEN DATEDIFF(year,
                              LAST_DAY(DATE_FROM_PARTS(dbp.birth_year, dbp.birth_month, 1)),
                              dbp.effective_start_date)
                ELSE DATEDIFF(year,
                              LAST_DAY(DATE_FROM_PARTS(dbp.birth_year, dbp.birth_month, 1)),
                              dbp.effective_start_date) - 1
            END
        ELSE NULL
    END AS age_at_least,
    dbp.death_year,
    dbp.death_date_approx,
    dbp.is_deceased,
    
    -- Age bands (calculated from age at period start)
    CASE
        WHEN dbp.age_at_period_start < 0 THEN 'Unknown'
        WHEN dbp.age_at_period_start >= 100 THEN '100+'
        ELSE TO_VARCHAR(FLOOR(dbp.age_at_period_start / 5) * 5) || '-' || TO_VARCHAR(FLOOR(dbp.age_at_period_start / 5) * 5 + 4)
    END AS age_band_5y,
    
    CASE
        WHEN dbp.age_at_period_start < 0 THEN 'Unknown'
        WHEN dbp.age_at_period_start >= 100 THEN '100+'
        ELSE TO_VARCHAR(FLOOR(dbp.age_at_period_start / 10) * 10) || '-' || TO_VARCHAR(FLOOR(dbp.age_at_period_start / 10) * 10 + 9)
    END AS age_band_10y,
    
    CASE
        WHEN dbp.age_at_period_start < 0 THEN 'Unknown'
        WHEN dbp.age_at_period_start < 5 THEN '0-4'
        WHEN dbp.age_at_period_start < 15 THEN '5-14'
        WHEN dbp.age_at_period_start < 25 THEN '15-24'
        WHEN dbp.age_at_period_start < 35 THEN '25-34'
        WHEN dbp.age_at_period_start < 45 THEN '35-44'
        WHEN dbp.age_at_period_start < 55 THEN '45-54'
        WHEN dbp.age_at_period_start < 65 THEN '55-64'
        WHEN dbp.age_at_period_start < 75 THEN '65-74'
        WHEN dbp.age_at_period_start < 85 THEN '75-84'
        ELSE '85+'
    END AS age_band_nhs,
    
    CASE
        WHEN dbp.age_at_period_start < 0 THEN 'Unknown'
        WHEN dbp.age_at_period_start < 5 THEN '0-4'
        WHEN dbp.age_at_period_start < 10 THEN '5-9'
        WHEN dbp.age_at_period_start < 15 THEN '10-14'
        WHEN dbp.age_at_period_start < 20 THEN '15-19'
        WHEN dbp.age_at_period_start < 25 THEN '20-24'
        WHEN dbp.age_at_period_start < 30 THEN '25-29'
        WHEN dbp.age_at_period_start < 35 THEN '30-34'
        WHEN dbp.age_at_period_start < 40 THEN '35-39'
        WHEN dbp.age_at_period_start < 45 THEN '40-44'
        WHEN dbp.age_at_period_start < 50 THEN '45-49'
        WHEN dbp.age_at_period_start < 55 THEN '50-54'
        WHEN dbp.age_at_period_start < 60 THEN '55-59'
        WHEN dbp.age_at_period_start < 65 THEN '60-64'
        WHEN dbp.age_at_period_start < 70 THEN '65-69'
        WHEN dbp.age_at_period_start < 75 THEN '70-74'
        WHEN dbp.age_at_period_start < 80 THEN '75-79'
        WHEN dbp.age_at_period_start < 85 THEN '80-84'
        ELSE '85+'
    END AS age_band_ons,
    
    CASE
        WHEN dbp.age_at_period_start < 0 THEN 'Unknown'
        WHEN dbp.age_at_period_start < 1 THEN 'Infant'
        WHEN dbp.age_at_period_start < 4 THEN 'Toddler'
        WHEN dbp.age_at_period_start < 13 THEN 'Child'
        WHEN dbp.age_at_period_start < 20 THEN 'Adolescent'
        WHEN dbp.age_at_period_start < 25 THEN 'Young Adult'
        WHEN dbp.age_at_period_start < 60 THEN 'Adult'
        WHEN dbp.age_at_period_start < 75 THEN 'Older Adult'
        WHEN dbp.age_at_period_start < 85 THEN 'Elderly'
        ELSE 'Very Elderly'
    END AS age_life_stage,
    
    -- Practice information (valid for this temporal period)
    dbp.practice_code,
    dbp.practice_name,
    dbp.registration_start_date,
    
    -- Static demographics (joined from existing tables)
    COALESCE(sex.sex, 'Unknown') AS sex,
    
    -- Temporal active status (from temporal periods)
    COALESCE(dbp.is_active, FALSE) AS is_active,
    dbp.inactive_reason,
    
    -- Temporal ethnicity (from temporal periods)
    COALESCE(dbp.ethnicity_category, 'Unknown') AS ethnicity_category,
    COALESCE(dbp.ethnicity_subcategory, 'Unknown') AS ethnicity_subcategory,
    COALESCE(dbp.ethnicity_granular, 'Unknown') AS ethnicity_granular,
    dbp.category_sort AS ethnicity_category_sort,
    dbp.display_sort_key AS ethnicity_display_sort_key,
    lang.language AS main_language,
    lang.language_type,
    lang.interpreter_type,
    COALESCE(lang.interpreter_needed, FALSE) AS interpreter_needed,
    
    -- Practice details  
    dp.pcn_code,
    dp.pcn_name,
    dp.pcn_name_with_borough,
    dp.practice_borough,
    dp.practice_postcode_dict AS practice_postcode,
    dp.practice_lsoa,
    dp.practice_msoa,
    dp.practice_latitude,
    dp.practice_longitude,
    nbhd.practice_neighbourhood,
    
    -- Address and Household Information (temporal tracking)
    dbp.post_code_hash,
    NULL AS uprn_hash,
    NULL::VARCHAR AS household_id,

    -- Geographic Placeholders (for future data)
    NULL AS lsoa_code_21,
    NULL AS lsoa_name_21,
    NULL AS ward_code,
    NULL AS ward_name,
    NULL::NUMBER AS imd_decile_19,
    NULL::VARCHAR AS imd_quintile_19,
    
    -- Patient neighbourhood placeholder
    NULL::VARCHAR AS patient_neighbourhood,
    
    -- Efficiency flags
    CASE WHEN dbp.effective_end_date IS NULL THEN TRUE ELSE FALSE END as is_current_period,
    CASE WHEN dbp.age_at_period_start != COALESCE(dbp.age_at_period_end, dbp.age_at_period_start) THEN TRUE ELSE FALSE END as age_changes_in_period

FROM demographics_by_period dbp

-- Join static demographics (assumed stable over time)
LEFT JOIN {{ ref('dim_person_sex') }} sex
    ON dbp.person_id = sex.person_id
    
-- Ethnicity is now temporal and included in demographics_by_period CTE
    
LEFT JOIN {{ ref('dim_person_main_language') }} lang
    ON dbp.person_id = lang.person_id

-- Join practice details
LEFT JOIN (
    SELECT 
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        pcn_name_with_borough,
        practice_borough,
        practice_postcode_dict,
        practice_lsoa,
        practice_msoa,
        practice_latitude,
        practice_longitude
    FROM {{ ref('dim_practice') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY practice_code ORDER BY practice_type_desc NULLS LAST) = 1
) dp ON dbp.practice_code = dp.practice_code

-- Join practice neighbourhood information
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} nbhd
    ON dbp.practice_code = nbhd.practice_code

-- Address is now included in the main demographics_by_period CTE with temporal tracking

-- Filter out persons without practice registration and valid birth dates (data quality requirements)
WHERE dbp.practice_code IS NOT NULL
  AND dbp.birth_date_approx IS NOT NULL

ORDER BY dbp.person_id, dbp.effective_start_date