{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'current_state'],
        cluster_by=['person_id'])
}}

/*
Current Person Demographics Dimension Table

Provides current demographic snapshot for ALL persons with registration history.
Built directly from intermediate tables for daily refresh accuracy.

Key Features:

• One row per person (current/latest demographics)

• Includes ALL persons with registration history (active and inactive)

• Daily refresh compatible - age calculated dynamically

• is_active flag shows current registration status

• Practice details show latest/current registration

Data Quality Filters:

• Excludes persons without birth dates (required for age)

• Excludes persons without any registration history

For historical analysis, use dim_person_demographics_historical.
*/

WITH current_registrations AS (
    -- Get the latest registration for each person
    SELECT 
        person_id,
        practice_ods_code as practice_code,
        practice_name,
        registration_start_date,
        registration_end_date,
        is_current_registration,
        is_latest_registration
    FROM {{ ref('int_patient_registrations') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id 
        ORDER BY 
            is_current_registration DESC,  -- Current registrations first
            registration_start_date DESC,   -- Then most recent
            registration_record_id DESC     -- Tie-breaker
    ) = 1
),


latest_ethnicity AS (
    -- Get the most recent ethnicity recording
    SELECT 
        person_id,
        ethnicity_category,
        ethnicity_subcategory,
        ethnicity_granular,
        category_sort as ethnicity_category_sort,
        display_sort_key as ethnicity_display_sort_key
    FROM {{ ref('int_ethnicity_all') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id 
        ORDER BY 
            clinical_effective_date DESC, 
            preference_rank ASC
    ) = 1
)

SELECT
    -- Core Identifiers
    bd.person_id,
    bd.sk_patient_id,

    -- Status Flags (key person attributes)
    COALESCE(cr.is_current_registration, FALSE) AS is_active,
    bd.is_deceased,
    bd.is_dummy_patient,
    CASE 
        WHEN bd.is_deceased THEN 'Deceased'
        WHEN cr.is_current_registration = FALSE THEN 'Registration ended'
        WHEN cr.practice_code IS NULL THEN 'No registration history'
        ELSE NULL
    END AS inactive_reason,

    -- Basic Demographics from dim_person_birth_death
    bd.birth_year,
    bd.birth_date_approx,
    CASE
        WHEN bd.birth_year IS NOT NULL AND bd.birth_month IS NOT NULL
            THEN LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1))
        ELSE NULL
    END AS birth_date_approx_end_of_month,
    bd.death_year,
    bd.death_date_approx,
    
    -- Age calculations (current as of today or death date)
    CASE 
        WHEN bd.is_deceased AND bd.death_date_approx IS NOT NULL 
            THEN FLOOR(DATEDIFF(month, bd.birth_date_approx, bd.death_date_approx) / 12)
        WHEN bd.birth_date_approx IS NOT NULL 
            THEN FLOOR(DATEDIFF(month, bd.birth_date_approx, CURRENT_DATE) / 12)
        ELSE NULL
    END AS age,
    
    -- Age at least (conservative calculation)
    CASE
        WHEN bd.birth_year IS NOT NULL AND bd.birth_month IS NOT NULL THEN
            CASE
                WHEN bd.is_deceased AND bd.death_date_approx IS NOT NULL THEN
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
                    -- Current age
                    CASE
                        WHEN CURRENT_DATE >= DATEADD(
                                year,
                                DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), CURRENT_DATE),
                                LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1))
                             )
                        THEN DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), CURRENT_DATE)
                        ELSE DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS(bd.birth_year, bd.birth_month, 1)), CURRENT_DATE) - 1
                    END
            END
        ELSE NULL
    END AS age_at_least,
    
    -- Age bands (calculated from current age)
    pa.age_band_5y,
    pa.age_band_10y,
    pa.age_band_nhs,
    pa.age_band_ons,
    pa.age_life_stage,
    
    -- School age flags
    pa.is_primary_school_age,
    pa.is_secondary_school_age,
    
    -- Sex from dim_person_sex
    COALESCE(sex.sex, 'Unknown') AS sex,

    -- Ethnicity from latest recording
    COALESCE(le.ethnicity_category, 'Unknown') AS ethnicity_category,
    COALESCE(le.ethnicity_subcategory, 'Unknown') AS ethnicity_subcategory,
    COALESCE(le.ethnicity_granular, 'Unknown') AS ethnicity_granular,
    le.ethnicity_category_sort,
    le.ethnicity_display_sort_key,

    -- Language and Communication from dim_person_main_language
    lang.language AS main_language,
    lang.language_type,
    lang.interpreter_type,
    COALESCE(lang.interpreter_needed, FALSE) AS interpreter_needed,
    
    -- Practice Registration (current or latest)
    cr.practice_code,
    cr.practice_name,
    cr.registration_start_date,
    cr.registration_end_date,
    
    -- PCN Information from dim_practice
    dp.pcn_code,
    dp.pcn_name,
    dp.pcn_name_with_borough,
    
    -- ICB Information from dim_practice
    dp.stp_code AS icb_code,
    dp.stp_name AS icb_name,
    
    -- Geographic Information from dim_practice
    dp.borough_registered,
    dp.practice_postcode_dict AS practice_postcode,
    dp.practice_lsoa,
    dp.practice_msoa,
    dp.practice_latitude,
    dp.practice_longitude,
    nbhd.neighbourhood_registered,
    
    -- Address Information
    ca.postcode_hash,
    NULL AS uprn_hash,  -- Placeholder for future
    NULL::VARCHAR AS household_id,  -- Placeholder for future

    -- Geographic Data (placeholders for future implementation)
    NULL AS lsoa_code_21,
    NULL AS lsoa_name_21,
    NULL AS ward_code,
    NULL AS ward_name,
    NULL::NUMBER AS imd_decile_19,
    NULL::VARCHAR AS imd_quintile_19,
    NULL::VARCHAR AS neighbourhood_resident

FROM {{ ref('dim_person_birth_death') }} bd

-- Join current/latest registration
LEFT JOIN current_registrations cr
    ON bd.person_id = cr.person_id

-- Join age information
LEFT JOIN {{ ref('dim_person_age') }} pa
    ON bd.person_id = pa.person_id

-- Join sex
LEFT JOIN {{ ref('dim_person_sex') }} sex
    ON bd.person_id = sex.person_id

-- Join language
LEFT JOIN {{ ref('dim_person_main_language') }} lang
    ON bd.person_id = lang.person_id

-- Join latest ethnicity
LEFT JOIN latest_ethnicity le
    ON bd.person_id = le.person_id

-- Join current address
LEFT JOIN {{ ref('int_person_postcode_hash') }} ca
    ON bd.person_id = ca.person_id

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
) dp ON cr.practice_code = dp.practice_code

-- Join practice neighbourhood
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} nbhd
    ON cr.practice_code = nbhd.practice_code

-- Filter: Must have birth date AND registration history
WHERE bd.birth_date_approx IS NOT NULL
  AND cr.person_id IS NOT NULL

ORDER BY bd.person_id