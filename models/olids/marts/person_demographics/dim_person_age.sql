{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'age'],
        cluster_by=['person_id'])
}}

-- Person age attributes and life stage categorisation

WITH age_calculations AS (
    -- Uses the birth/death dimension table and calculates age based on appropriate date
    -- (death date for deceased persons, current date for living persons)
    SELECT
        bd.person_id,
        bd.sk_patient_id,
        bd.birth_year,
        bd.birth_month,
        bd.birth_date_approx,
        bd.death_year,
        bd.death_month,
        bd.death_date_approx,
        bd.is_deceased,
        -- Use death date for calculations if deceased, otherwise current date
        CASE
            WHEN bd.is_deceased THEN bd.death_date_approx
            ELSE CURRENT_DATE()
        END AS calculation_date,
        -- Calculate age metrics based on appropriate end date
        FLOOR(DATEDIFF(month, bd.birth_date_approx,
            CASE WHEN bd.is_deceased THEN bd.death_date_approx ELSE CURRENT_DATE() END
        ) / 12) AS age,
        DATEDIFF(month, bd.birth_date_approx,
            CASE WHEN bd.is_deceased THEN bd.death_date_approx ELSE CURRENT_DATE() END
        ) AS age_months,
        FLOOR(DATEDIFF(day, bd.birth_date_approx,
            CASE WHEN bd.is_deceased THEN bd.death_date_approx ELSE CURRENT_DATE() END
        ) / 7) AS age_weeks_approx,
        DATEDIFF(day, bd.birth_date_approx,
            CASE WHEN bd.is_deceased THEN bd.death_date_approx ELSE CURRENT_DATE() END
        ) AS age_days_approx,
        -- Academic year calculation for school stages
        CASE
            WHEN EXTRACT(MONTH FROM CURRENT_DATE()) >= 9 THEN EXTRACT(YEAR FROM CURRENT_DATE())
            ELSE EXTRACT(YEAR FROM CURRENT_DATE()) - 1
        END AS academic_year_start
    FROM {{ ref('dim_person_birth_death') }} bd
)

-- Final SELECT statement to assemble all age-related attributes
SELECT
    ac.person_id,
    ac.sk_patient_id,
    ac.birth_year,
    ac.birth_month,
    ac.birth_date_approx,
    -- Approximate date of birth using the last day of the recorded birth month
    CASE
        WHEN ac.birth_year IS NOT NULL AND ac.birth_month IS NOT NULL
            THEN LAST_DAY(DATE_FROM_PARTS(ac.birth_year, ac.birth_month, 1))
        ELSE NULL
    END AS birth_date_approx_end_of_month,
    ac.death_year,
    ac.death_month,
    ac.death_date_approx,
    ac.is_deceased,
    ac.age,
    -- Minimum possible age in full years using day-accurate logic with DOB assumed as last day of birth month
    CASE
        WHEN ac.birth_year IS NOT NULL AND ac.birth_month IS NOT NULL THEN
            CASE
                WHEN ac.calculation_date >= DATEADD(
                        year,
                        DATEDIFF(year,
                                 LAST_DAY(DATE_FROM_PARTS(ac.birth_year, ac.birth_month, 1)),
                                 ac.calculation_date),
                        LAST_DAY(DATE_FROM_PARTS(ac.birth_year, ac.birth_month, 1))
                     )
                THEN DATEDIFF(year,
                              LAST_DAY(DATE_FROM_PARTS(ac.birth_year, ac.birth_month, 1)),
                              ac.calculation_date)
                ELSE DATEDIFF(year,
                              LAST_DAY(DATE_FROM_PARTS(ac.birth_year, ac.birth_month, 1)),
                              ac.calculation_date) - 1
            END
        ELSE NULL
    END AS age_at_least,
    ac.age_months,
    ac.age_weeks_approx,
    ac.age_days_approx,

    -- 5-year age bands
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age >= 85 THEN '85+'
        ELSE TO_VARCHAR(FLOOR(ac.age / 5) * 5) || '-' || TO_VARCHAR(FLOOR(ac.age / 5) * 5 + 4)
    END AS age_band_5y,

    -- 10-year age bands
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age >= 80 THEN '80+'
        ELSE TO_VARCHAR(FLOOR(ac.age / 10) * 10) || '-' || TO_VARCHAR(FLOOR(ac.age / 10) * 10 + 9)
    END AS age_band_10y,


    -- NHS Digital age bands (used in Health Survey for England)
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age < 5 THEN '0-4'
        WHEN ac.age < 15 THEN '5-14'
        WHEN ac.age < 25 THEN '15-24'
        WHEN ac.age < 35 THEN '25-34'
        WHEN ac.age < 45 THEN '35-44'
        WHEN ac.age < 55 THEN '45-54'
        WHEN ac.age < 65 THEN '55-64'
        WHEN ac.age < 75 THEN '65-74'
        WHEN ac.age < 85 THEN '75-84'
        ELSE '85+'
    END AS age_band_nhs,

    -- ONS (Office for National Statistics) age bands
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age < 5 THEN '0-4'
        WHEN ac.age < 10 THEN '5-9'
        WHEN ac.age < 15 THEN '10-14'
        WHEN ac.age < 20 THEN '15-19'
        WHEN ac.age < 25 THEN '20-24'
        WHEN ac.age < 30 THEN '25-29'
        WHEN ac.age < 35 THEN '30-34'
        WHEN ac.age < 40 THEN '35-39'
        WHEN ac.age < 45 THEN '40-44'
        WHEN ac.age < 50 THEN '45-49'
        WHEN ac.age < 55 THEN '50-54'
        WHEN ac.age < 60 THEN '55-59'
        WHEN ac.age < 65 THEN '60-64'
        WHEN ac.age < 70 THEN '65-69'
        WHEN ac.age < 75 THEN '70-74'
        WHEN ac.age < 80 THEN '75-79'
        WHEN ac.age < 85 THEN '80-84'
        ELSE '85+'
    END AS age_band_ons,

    -- Life stage categories
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age < 1 THEN 'Infant'
        WHEN ac.age < 4 THEN 'Toddler'
        WHEN ac.age < 13 THEN 'Child'
        WHEN ac.age < 20 THEN 'Adolescent'
        WHEN ac.age < 25 THEN 'Young Adult'
        WHEN ac.age < 60 THEN 'Adult'
        WHEN ac.age < 75 THEN 'Older Adult'
        WHEN ac.age < 85 THEN 'Elderly'
        ELSE 'Very Elderly'
    END AS age_life_stage,

    -- UK school year/stage based on age at academic year start
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age < 3 THEN 'Pre-school'
        WHEN ac.age = 3 OR (ac.age = 4 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 48) THEN 'Nursery'
        WHEN ac.age = 4 OR (ac.age = 5 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 60) THEN 'Reception'
        WHEN ac.age = 5 OR (ac.age = 6 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 72) THEN 'Year 1'
        WHEN ac.age = 6 OR (ac.age = 7 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 84) THEN 'Year 2'
        WHEN ac.age = 7 OR (ac.age = 8 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 96) THEN 'Year 3'
        WHEN ac.age = 8 OR (ac.age = 9 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 108) THEN 'Year 4'
        WHEN ac.age = 9 OR (ac.age = 10 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 120) THEN 'Year 5'
        WHEN ac.age = 10 OR (ac.age = 11 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 132) THEN 'Year 6'
        WHEN ac.age = 11 OR (ac.age = 12 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 144) THEN 'Year 7'
        WHEN ac.age = 12 OR (ac.age = 13 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 156) THEN 'Year 8'
        WHEN ac.age = 13 OR (ac.age = 14 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 168) THEN 'Year 9'
        WHEN ac.age = 14 OR (ac.age = 15 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 180) THEN 'Year 10'
        WHEN ac.age = 15 OR (ac.age = 16 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 192) THEN 'Year 11'
        WHEN ac.age = 16 OR (ac.age = 17 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 204) THEN 'Year 12'
        WHEN ac.age = 17 OR (ac.age = 18 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 216) THEN 'Year 13'
        WHEN ac.age >= 18 THEN 'Post-secondary'
        ELSE 'Unknown'
    END AS age_school_stage,

    -- Broader education level category
    CASE
        WHEN ac.age < 0 THEN 'Unknown'
        WHEN ac.age < 3 THEN 'Pre-school'
        WHEN ac.age = 3 OR (ac.age = 4 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 48) THEN 'Nursery'
        WHEN ac.age >= 4 AND (ac.age < 11 OR (ac.age = 11 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 132)) THEN 'Primary School'
        WHEN ac.age >= 11 AND (ac.age < 16 OR (ac.age = 16 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 192)) THEN 'Secondary School - KS3/KS4'
        WHEN ac.age >= 16 AND (ac.age < 18 OR (ac.age = 18 AND DATEDIFF(month, ac.birth_date_approx, DATE_FROM_PARTS(ac.academic_year_start, 9, 1)) < 216)) THEN 'Secondary School - Sixth Form'
        WHEN ac.age >= 18 THEN 'Post-secondary'
        ELSE 'Unknown'
    END AS age_education_level,

    -- School age flags using reusable macro with UK academic year logic
    {{ calculate_school_age_flags('ac.birth_date_approx', 'ac.calculation_date') }}

FROM age_calculations ac
