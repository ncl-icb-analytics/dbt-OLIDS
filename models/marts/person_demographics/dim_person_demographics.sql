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
- Current practice registration
- Enhanced practice and PCN information including borough context
- Practice neighbourhood and organisational hierarchy
- Geographic data from Dictionary sources and placeholders for future data

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
    age.death_year,
    age.death_date_approx,
    age.age,
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

    -- Language and Communication
    lang.language AS main_language,
    lang.language_category,
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
    nbhd.local_authority,
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
    NULL::VARCHAR AS imd_quintile_19

FROM {{ ref('dim_person_age') }} AS age

-- Join demographics
LEFT JOIN {{ ref('dim_person_sex') }} AS sex
    ON age.person_id = sex.person_id

LEFT JOIN {{ ref('dim_person_ethnicity') }} AS eth
    ON age.person_id = eth.person_id

LEFT JOIN {{ ref('dim_person_main_language') }} AS lang
    ON age.person_id = lang.person_id

-- Join practice information
LEFT JOIN {{ ref('dim_person_current_practice') }} AS prac
    ON age.person_id = prac.person_id

-- Join enhanced practice dimension (includes PCN and borough information)
LEFT JOIN {{ ref('dim_practice') }} AS dp
    ON prac.practice_code = dp.practice_code

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
