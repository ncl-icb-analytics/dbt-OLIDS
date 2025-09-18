{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'historical', 'person_month'],
        cluster_by=['analysis_month', 'person_id'])
}}

/*
Historical Person Demographics - Person-Month Grain

Provides person-month demographic snapshots for the last 5 years.
One row per person per month with accurate age and demographics for that specific month.

Key Features:

• Person-month grain for accurate temporal analysis

• Age calculated precisely for each month

• Practice registration tracked monthly

• Ethnicity captured as recorded by each month

• Optimised for joining with person_month_analysis_base

• 5-year history window for performance

Data Quality:

• Only includes persons with valid birth dates

• Only includes months where person had a registration

For current state only, use dim_person_demographics.
*/

WITH person_months AS (
    -- Generate person-month records for all registered periods
    SELECT DISTINCT
        ds.month_end_date as analysis_month,
        hr.person_id,
        hr.practice_code,
        hr.practice_name,
        hr.registration_start_date,
        hr.registration_end_date,
        hr.is_current_registration,
        -- Person is active if registered for any part of the month
        CASE 
            WHEN hr.registration_end_date IS NULL THEN TRUE
            WHEN hr.registration_end_date > ds.month_end_date THEN TRUE
            WHEN hr.registration_end_date < hr.registration_start_date THEN TRUE  -- Data quality handling
            ELSE FALSE
        END AS is_active
    FROM {{ ref('dim_person_historical_practice') }} hr
    INNER JOIN {{ ref('int_date_spine') }} ds
        ON hr.registration_start_date <= ds.month_end_date
        AND (hr.registration_end_date IS NULL OR hr.registration_end_date >= ds.month_start_date)
        AND ds.month_end_date >= DATEADD('month', -60, CURRENT_DATE)  -- 5 year limit
        AND ds.month_end_date <= LAST_DAY(CURRENT_DATE)  -- Don't create future months
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ds.month_end_date, hr.person_id 
        ORDER BY hr.is_current_registration DESC, hr.registration_start_date DESC
    ) = 1
),

monthly_addresses AS (
    -- Get address valid for each person-month using intermediate model with SCD2 logic
    SELECT DISTINCT
        pm.analysis_month,
        pm.person_id,
        pc.postcode_hash
    FROM person_months pm
    LEFT JOIN {{ ref('int_person_postcode_hash') }} pc
        ON pm.person_id = pc.person_id
        AND pc.address_start_date <= pm.analysis_month
        AND (pc.address_end_date IS NULL OR pc.address_end_date >= DATE_TRUNC('month', pm.analysis_month))
),

monthly_ethnicity AS (
    -- Get ethnicity as recorded by each month
    SELECT 
        pm.analysis_month,
        pm.person_id,
        ea.ethnicity_category,
        ea.ethnicity_subcategory,
        ea.ethnicity_granular,
        ea.category_sort as ethnicity_category_sort,
        ea.display_sort_key as ethnicity_display_sort_key
    FROM person_months pm
    INNER JOIN {{ ref('int_ethnicity_all') }} ea
        ON pm.person_id = ea.person_id
        AND ea.clinical_effective_date <= pm.analysis_month
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pm.analysis_month, pm.person_id
        ORDER BY ea.clinical_effective_date DESC, ea.preference_rank ASC
    ) = 1
)

SELECT
    -- Core identifiers
    pm.analysis_month,
    pm.person_id,
    bd.sk_patient_id,
    
    -- Status Flags (key person attributes for this month)
    pm.is_active,
    bd.is_deceased,
    bd.is_dummy_patient,
    CASE 
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month THEN 'Deceased'
        WHEN pm.is_active = FALSE THEN 'Registration ended'
        ELSE NULL
    END AS inactive_reason,
    
    -- Birth and death information
    bd.birth_year,
    bd.birth_month,
    bd.birth_date_approx,
    -- Additional birth date format for legacy compatibility
    CASE
        WHEN bd.birth_year IS NOT NULL AND bd.birth_month IS NOT NULL
            THEN LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1))
        ELSE NULL
    END AS birth_date_approx_end_of_month,
    bd.death_year,
    bd.death_month,
    bd.death_date_approx,
    
    -- Age calculated for this specific month
    CASE 
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month 
            THEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12)
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12)
        ELSE NULL
    END AS age,
    
    -- Conservative age calculation for this month
    CASE
        WHEN bd.birth_year IS NOT NULL AND bd.birth_month IS NOT NULL THEN
            CASE
                WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month THEN
                    -- Age at death
                    CASE
                        WHEN bd.death_date_approx >= DATEADD(
                                year,
                                DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), bd.death_date_approx),
                                LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1))
                             )
                        THEN DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), bd.death_date_approx)
                        ELSE DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), bd.death_date_approx) - 1
                    END
                ELSE
                    -- Age for this month (using consistent reference date)
                    CASE
                        WHEN DATE_TRUNC('month', pm.analysis_month) >= DATEADD(
                                year,
                                DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), DATE_TRUNC('month', pm.analysis_month)),
                                LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1))
                             )
                        THEN DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), DATE_TRUNC('month', pm.analysis_month))
                        ELSE DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), DATE_TRUNC('month', pm.analysis_month)) - 1
                    END
            END
        ELSE NULL
    END AS age_at_least,
    
    -- Age bands calculated from age for this month
    CASE
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) >= 100 THEN '100+'
                ELSE TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) / 5) * 5) || '-' || 
                     TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) / 5) * 5 + 4)
            END
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) >= 100 THEN '100+'
                ELSE TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) / 5) * 5) || '-' || 
                     TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) / 5) * 5 + 4)
            END
        ELSE 'Unknown'
    END AS age_band_5y,
    
    CASE
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) >= 100 THEN '100+'
                ELSE TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) / 10) * 10) || '-' || 
                     TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) / 10) * 10 + 9)
            END
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) >= 100 THEN '100+'
                ELSE TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) / 10) * 10) || '-' || 
                     TO_VARCHAR(FLOOR(FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) / 10) * 10 + 9)
            END
        ELSE 'Unknown'
    END AS age_band_10y,
    
    -- NHS age bands
    CASE
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 5 THEN '0-4'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 15 THEN '5-14'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 25 THEN '15-24'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 35 THEN '25-34'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 45 THEN '35-44'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 55 THEN '45-54'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 65 THEN '55-64'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 75 THEN '65-74'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 85 THEN '75-84'
                ELSE '85+'
            END
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 5 THEN '0-4'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 15 THEN '5-14'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 25 THEN '15-24'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 35 THEN '25-34'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 45 THEN '35-44'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 55 THEN '45-54'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 65 THEN '55-64'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 75 THEN '65-74'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 85 THEN '75-84'
                ELSE '85+'
            END
        ELSE 'Unknown'
    END AS age_band_nhs,
    
    -- ONS age bands
    CASE
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 5 THEN '0-4'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 10 THEN '5-9'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 15 THEN '10-14'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 20 THEN '15-19'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 25 THEN '20-24'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 30 THEN '25-29'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 35 THEN '30-34'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 40 THEN '35-39'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 45 THEN '40-44'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 50 THEN '45-49'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 55 THEN '50-54'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 60 THEN '55-59'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 65 THEN '60-64'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 70 THEN '65-69'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 75 THEN '70-74'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 80 THEN '75-79'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 85 THEN '80-84'
                ELSE '85+'
            END
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 5 THEN '0-4'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 10 THEN '5-9'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 15 THEN '10-14'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 20 THEN '15-19'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 25 THEN '20-24'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 30 THEN '25-29'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 35 THEN '30-34'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 40 THEN '35-39'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 45 THEN '40-44'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 50 THEN '45-49'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 55 THEN '50-54'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 60 THEN '55-59'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 65 THEN '60-64'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 70 THEN '65-69'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 75 THEN '70-74'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 80 THEN '75-79'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 85 THEN '80-84'
                ELSE '85+'
            END
        ELSE 'Unknown'
    END AS age_band_ons,
    
    -- Life stage
    CASE
        WHEN bd.is_deceased AND bd.death_date_approx <= pm.analysis_month 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 1 THEN 'Infant'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 4 THEN 'Toddler'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 13 THEN 'Child'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 20 THEN 'Adolescent'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 25 THEN 'Young Adult'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 60 THEN 'Adult'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 75 THEN 'Older Adult'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12) < 85 THEN 'Elderly'
                ELSE 'Very Elderly'
            END
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN CASE
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 0 THEN 'Unknown'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 1 THEN 'Infant'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 4 THEN 'Toddler'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 13 THEN 'Child'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 20 THEN 'Adolescent'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 25 THEN 'Young Adult'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 60 THEN 'Adult'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 75 THEN 'Older Adult'
                WHEN FLOOR(DATEDIFF(month, bd.birth_date_approx, DATE_TRUNC('month', pm.analysis_month)) / 12) < 85 THEN 'Elderly'
                ELSE 'Very Elderly'
            END
        ELSE 'Unknown'
    END AS age_life_stage,
    
    -- School age flags (calculated for this analysis month using UK academic year logic)
    {{ calculate_school_age_flags('bd.birth_date_approx', 'pm.analysis_month') }},
    
    -- Sex
    COALESCE(sex.sex, 'Unknown') AS sex,
    
    -- Ethnicity as recorded by this month
    COALESCE(me.ethnicity_category, 'Unknown') AS ethnicity_category,
    COALESCE(me.ethnicity_subcategory, 'Unknown') AS ethnicity_subcategory,
    COALESCE(me.ethnicity_granular, 'Unknown') AS ethnicity_granular,
    me.ethnicity_category_sort,
    me.ethnicity_display_sort_key,
    
    -- Language
    lang.language AS main_language,
    lang.language_type,
    lang.interpreter_type,
    COALESCE(lang.interpreter_needed, FALSE) AS interpreter_needed,
    
    -- Practice registration for this month
    pm.practice_code,
    pm.practice_name,
    pm.registration_start_date,
    pm.registration_end_date,
    
    -- PCN Information
    dp.pcn_code,
    dp.pcn_name,
    dp.pcn_name_with_borough,
    
    -- ICB Information
    dp.stp_code AS icb_code,
    dp.stp_name AS icb_name,
    
    -- Geographic Information
    dp.borough_registered,
    dp.practice_postcode_dict AS practice_postcode,
    dp.practice_lsoa,
    dp.practice_msoa,
    dp.practice_latitude,
    dp.practice_longitude,
    nbhd.neighbourhood_registered,
    
    -- Address for this month
    ma.postcode_hash,
    NULL AS uprn_hash,  -- Placeholder
    NULL::VARCHAR AS household_id,  -- Placeholder

    -- Geographic placeholders
    NULL AS lsoa_code_21,
    NULL AS lsoa_name_21,
    NULL AS ward_code,
    NULL AS ward_name,
    NULL::NUMBER AS imd_decile_19,
    NULL::VARCHAR AS imd_quintile_19,
    NULL::VARCHAR AS neighbourhood_resident,
    
    -- SCD2 compatibility fields for person-month grain
    -- For person-month, we treat each month as a separate "period" that spans the full month
    CASE WHEN pm.analysis_month = LAST_DAY(CURRENT_DATE) THEN TRUE ELSE FALSE END as is_current_period,
    DATE_TRUNC('month', pm.analysis_month) as effective_start_date,  -- First day of month
    CASE 
        WHEN pm.analysis_month = LAST_DAY(CURRENT_DATE) THEN NULL  -- Current month has no end date
        ELSE pm.analysis_month  -- Month ends on analysis_month (last day of month)
    END as effective_end_date,
    ROW_NUMBER() OVER (PARTITION BY pm.person_id ORDER BY pm.analysis_month) as period_sequence,
    -- Age changes when birth month anniversary occurs within the month
    CASE 
        WHEN bd.birth_month = EXTRACT(month FROM pm.analysis_month) THEN TRUE 
        ELSE FALSE 
    END as age_changes_in_period

FROM person_months pm

-- Join birth/death information
INNER JOIN {{ ref('dim_person_birth_death') }} bd
    ON pm.person_id = bd.person_id

-- Join sex
LEFT JOIN {{ ref('dim_person_sex') }} sex
    ON pm.person_id = sex.person_id

-- Join language
LEFT JOIN {{ ref('dim_person_main_language') }} lang
    ON pm.person_id = lang.person_id

-- Join monthly ethnicity
LEFT JOIN monthly_ethnicity me
    ON pm.analysis_month = me.analysis_month
    AND pm.person_id = me.person_id

-- Join monthly address
LEFT JOIN monthly_addresses ma
    ON pm.analysis_month = ma.analysis_month
    AND pm.person_id = ma.person_id

-- Join practice details
LEFT JOIN (
    SELECT 
        practice_code,
        practice_name,
        pcn_code,
        pcn_name,
        pcn_name_with_borough,
        borough_registered,
        practice_postcode_dict,
        practice_lsoa,
        practice_msoa,
        practice_latitude,
        practice_longitude,
        stp_code,
        stp_name
    FROM {{ ref('dim_practice') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY practice_code ORDER BY practice_type_desc NULLS LAST) = 1
) dp ON pm.practice_code = dp.practice_code

-- Join practice neighbourhood
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} nbhd
    ON pm.practice_code = nbhd.practice_code

-- Filter: Must have birth date
WHERE bd.birth_date_approx IS NOT NULL

ORDER BY pm.analysis_month, pm.person_id