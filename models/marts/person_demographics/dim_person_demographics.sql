{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'comprehensive'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Comprehensive demographics dimension table providing a single source of truth for person demographics. Consolidates information from multiple dimension tables: Age and birth/death information, Sex demographics, Ethnicity details, Language and communication needs, Current practice registration, Practice neighbourhood and organisational hierarchy, Geographic placeholders for future LSOA, ward, and deprivation data. Serves as the primary demographics reference for analytics and reporting.'"
        ]
    )
}}

/*
Comprehensive Person Demographics Dimension Table
Provides a single source of truth for person demographics by consolidating information from:
- Age and birth/death information  
- Sex demographics
- Ethnicity details
- Language and communication needs
- Current practice registration
- Practice neighbourhood and organisational hierarchy
- Geographic placeholders for future data (LSOA 2021, ward, IMD 2019)

Note: Working with dummy data so some geographic fields will be NULL until proper data is available.
Geographic fields include version numbers (LSOA_21, IMD_19) to support historical comparisons when new versions become available.
*/

WITH current_addresses AS (
    -- Get the most recent address for each person
    SELECT 
        pp.person_id,
        pa.post_code_hash,
        -- UPRN hash will be available in real data, placeholder for now
        NULL AS uprn_hash,
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id 
            ORDER BY pa.start_date DESC, pa.lds_datetime_data_acquired DESC
        ) AS address_rank
    FROM {{ ref('stg_olids_patient_person') }} pp
    JOIN {{ ref('stg_olids_patient_address') }} pa
        ON pp.patient_id = pa.patient_id
    WHERE pa.end_date IS NULL OR pa.end_date >= CURRENT_DATE()
)

SELECT
    -- Core Identifiers
    age.person_id,
    age.sk_patient_id,
    
    -- Status Flags
    COALESCE(active.is_active, FALSE) AS is_active,
    age.is_deceased,
    
    -- Basic Demographics
    COALESCE(sex.sex, 'Unknown') AS sex,
    age.birth_year,
    age.birth_date_approx,
    age.death_year,
    age.death_date_approx,
    
    -- Age and Age Bands
    age.age,
    age.age_life_stage,
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
    COALESCE(lang.interpreter_needed, FALSE) AS interpreter_needed,
    lang.interpreter_type,
    
    -- Current Practice Registration
    prac.practice_code AS current_practice_code,
    prac.practice_name AS current_practice_name,
    prac.practice_postcode AS current_practice_postcode,
    prac.registration_start_date,
    
    -- Practice Neighbourhood and Organisational Hierarchy
    pcn.pcn_code,
    pcn.pcn_name,
    nbhd.local_authority,
    nbhd.practice_neighbourhood,
    
    -- Geographic Information (Current and placeholders for future data)
    addr.post_code_hash,
    addr.uprn_hash,
    
    -- Household linkage (will be populated when real UPRN data available)
    NULL::VARCHAR AS household_id,  -- Link to dim_households via UPRN hash
    
    NULL AS lsoa_code_21,
    NULL AS lsoa_name_21,
    NULL AS ward_code,
    NULL AS ward_name,
    NULL::NUMBER AS imd_decile_19,
    NULL::NUMBER AS imd_quintile_19

FROM {{ ref('dim_person_age') }} age

-- Join demographics
LEFT JOIN {{ ref('dim_person_sex') }} sex
    ON age.person_id = sex.person_id

LEFT JOIN {{ ref('dim_person_ethnicity') }} eth
    ON age.person_id = eth.person_id

LEFT JOIN {{ ref('dim_person_main_language') }} lang
    ON age.person_id = lang.person_id

-- Join practice information
LEFT JOIN {{ ref('dim_person_current_practice') }} prac
    ON age.person_id = prac.person_id

-- Join practice neighbourhood information
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} nbhd
    ON prac.practice_code = nbhd.practice_code

-- Join practice PCN and commissioning information
LEFT JOIN {{ ref('dim_practice_pcn') }} pcn
    ON prac.practice_code = pcn.practice_code

-- Join active patient status
LEFT JOIN {{ ref('dim_person_active_patients') }} active
    ON age.person_id = active.person_id

-- Join current address information
LEFT JOIN current_addresses addr
    ON age.person_id = addr.person_id
    AND addr.address_rank = 1 