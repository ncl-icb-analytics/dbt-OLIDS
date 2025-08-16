{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'current_state'],
        cluster_by=['person_id'])
}}

/*
Current Person Demographics Dimension Table

This is a current-state view derived from dim_person_demographics_historical.
Provides a simplified interface for current demographics by selecting only the 
current period records from the historical SCD2 table.

Key Features:

• Current-state snapshot (is_current_period = TRUE)

• Consistent business logic with historical table 

• Single source of truth - all logic maintained in historical model

• Optimised for current-state analytics without temporal complexity

• Automatic consistency with historical data

For historical analysis, use dim_person_demographics_historical directly.
For current demographics, use this view for simplicity.
*/

SELECT
    -- Core Identifiers
    person_id,
    sk_patient_id,

    -- Basic Demographics
    birth_year,
    birth_date_approx,
    birth_date_approx_end_of_month,
    age_at_least,
    death_year,
    death_date_approx,
    is_deceased,
    age,
    age_band_5y,
    age_band_10y,
    age_band_nhs,
    age_band_ons,
    age_life_stage,
    sex,

    -- Active Status
    is_active,
    inactive_reason,

    -- Ethnicity
    ethnicity_category,
    ethnicity_subcategory,
    ethnicity_granular,
    ethnicity_category_sort,
    ethnicity_display_sort_key,

    -- Language and Communication
    main_language,
    language_type,
    interpreter_type,
    interpreter_needed,
    
    -- Practice Registration
    practice_code,
    practice_name,
    registration_start_date,
    
    -- PCN Information
    pcn_code,
    pcn_name,
    pcn_name_with_borough,
    
    -- Geographic Information
    practice_borough,
    practice_postcode,
    practice_lsoa,
    practice_msoa,
    practice_latitude,
    practice_longitude,
    practice_neighbourhood,
    
    -- Address and Household Information
    post_code_hash,
    uprn_hash,
    household_id,

    -- Geographic Data (for future implementation)
    lsoa_code_21,
    lsoa_name_21,
    ward_code,
    ward_name,
    imd_decile_19,
    imd_quintile_19,
    patient_neighbourhood,
    
    -- Temporal metadata (for reference)
    effective_start_date,
    period_sequence

FROM {{ ref('dim_person_demographics_historical') }}
WHERE is_current_period = TRUE