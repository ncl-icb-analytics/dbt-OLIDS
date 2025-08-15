{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'comprehensive'],
        cluster_by=['person_id'])
}}

/*
Comprehensive Person Demographics Dimension Table
Provides a single source of truth for person demographics by consolidating information from:
- Age and birth/death information
- Sex demographics
- Ethnicity details
- Language and communication needs
- Practice registration (current or most recent historical - required)
- Enhanced practice and PCN information including borough context
- Practice neighbourhood and organisational hierarchy
- Geographic data from Dictionary sources and placeholders for future data

Note: Persons without practice registration information are excluded to ensure data completeness.
Includes both standard PCN names and borough-prefixed variants for North Central London context.
Geographic fields include version numbers (LSOA_21, IMD_19) to support historical comparisons when new versions become available.
*/

WITH current_patient_per_person AS (
    -- Get the patient_id for current GP registration for each person
    SELECT
        ipr.person_id,
        ipr.patient_id,
        ipr.sk_patient_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ipr.person_id
        ORDER BY ipr.registration_start_date DESC, ipr.episode_of_care_id DESC
    ) = 1
),

current_addresses AS (
    -- Get the most recent address for each person using their current patient record
    SELECT
        cpp.person_id,
        pa.post_code_hash,
        -- UPRN hash will be available in real data, placeholder for now
        NULL AS uprn_hash,
        ROW_NUMBER() OVER (
            PARTITION BY cpp.person_id
            ORDER BY pa.start_date DESC, pa.lds_datetime_data_acquired DESC
        ) AS address_rank
    FROM current_patient_per_person AS cpp
    INNER JOIN {{ ref('stg_olids_patient_address') }} AS pa
        ON cpp.patient_id = pa.patient_id
    WHERE pa.end_date IS NULL OR pa.end_date >= CURRENT_DATE()
)

,

-- Choose current practice if present; otherwise latest historical from dim_person_historical_practice
chosen_practice AS (
    SELECT
        person_id,
        practice_code,
        practice_name,
        registration_start_date
    FROM {{ ref('dim_person_historical_practice') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY is_current_registration DESC, registration_start_date DESC, effective_end_date DESC
    ) = 1
)

SELECT
    -- Core Identifiers
    age.person_id,
    age.sk_patient_id,

    -- Status Flags
    COALESCE(active.is_active, FALSE) AS is_active,
    age.is_deceased,
    COALESCE(sex.sex, 'Unknown') AS sex,

    -- Basic Demographics
    age.birth_year,
    age.birth_date_approx,
    age.birth_date_approx_end_of_month,
    age.death_year,
    age.death_date_approx,
    age.age,
    age.age_at_least,
    age.age_life_stage,

    -- Age Bands
    age.age_band_5y,
    age.age_band_10y,
    age.age_band_nhs,
    age.age_band_ons,

    -- Ethnicity
    eth.ethnicity_category,
    eth.ethnicity_subcategory,
    eth.ethnicity_granular,
    -- Ethnicity sorting helpers
    eth.category_sort AS ethnicity_category_sort,
    eth.display_sort_key AS ethnicity_display_sort_key,

    -- Language and Communication
    lang.language AS main_language,
    lang.language_type,
    lang.interpreter_type,
    COALESCE(lang.interpreter_needed, FALSE) AS interpreter_needed,
    
    -- Practice Registration
    prac.practice_code,
    prac.practice_name,
    prac.registration_start_date,
    
    -- PCN Information
    dp.pcn_code,
    dp.pcn_name,
    dp.pcn_name_with_borough,
    
    -- Geographic Information
    dp.practice_borough,
    dp.practice_postcode_dict AS practice_postcode,
    dp.practice_lsoa,
    dp.practice_msoa,
    dp.practice_latitude,
    dp.practice_longitude,
    nbhd.practice_neighbourhood,
    
    -- Address and Household Information
    addr.post_code_hash,
    addr.uprn_hash,
    NULL::VARCHAR AS household_id,

    -- Geographic Placeholders (for future data)
    NULL AS lsoa_code_21,
    NULL AS lsoa_name_21,
    NULL AS ward_code,
    NULL AS ward_name,
    NULL::NUMBER AS imd_decile_19,
    NULL::VARCHAR AS imd_quintile_19,
    
    -- Patient neighbourhood placeholder
    NULL::VARCHAR AS patient_neighbourhood

FROM {{ ref('dim_person_age') }} AS age

-- Join demographics
LEFT JOIN {{ ref('dim_person_sex') }} AS sex
    ON age.person_id = sex.person_id

LEFT JOIN {{ ref('dim_person_ethnicity') }} AS eth
    ON age.person_id = eth.person_id

LEFT JOIN {{ ref('dim_person_main_language') }} AS lang
    ON age.person_id = lang.person_id

-- Join chosen practice (current if available, otherwise latest historical)
-- INNER JOIN to ensure all persons have practice information
INNER JOIN chosen_practice AS prac
    ON age.person_id = prac.person_id

-- Join enhanced practice dimension (includes PCN and borough information)
-- Deduplicate dim_practice which has 2 rows per practice_code
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
) AS dp ON prac.practice_code = dp.practice_code

-- Join practice neighbourhood information
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} AS nbhd
    ON prac.practice_code = nbhd.practice_code

-- Join active patient status
LEFT JOIN {{ ref('dim_person_active_patients') }} AS active
    ON age.person_id = active.person_id

-- Join current address information
LEFT JOIN current_addresses AS addr
    ON
        age.person_id = addr.person_id
        AND addr.address_rank = 1
