{{
  config(
    materialized='table',
    tags=['household', 'geography', 'bridge', 'members'],
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: Household Members - Bridge table linking patients to households for geographic and social analysis.

Business Purpose:
• Support population health teams in analysing household-level health outcomes and social determinants
• Enable business intelligence reporting on geographic distribution of patients and practice populations
• Provide foundation for household-based care planning and resource allocation
• Support neighbourhood-level health analytics and community health assessments

Data Granularity:
• One row per person per household (UPRN-based dwelling)
• Includes all patients with valid household addresses
• Current snapshot of household membership with demographic attributes

Key Features:
• Links patients to geographic households and practice organisational structures
• Includes demographic characteristics for household composition analysis
• Supports spatial analysis combining patient attributes with dwelling location
• Enables population health reporting at household and neighbourhood levels'"
    ]
  )
}}

/*
Household Members Bridge Table
Links people to households (via UPRN hash) and includes:
- Practice registration details
- Demographics and status information
- Temporal aspects of household membership

This separates concerns between:
- dim_households: Physical dwelling properties
- fct_household_members: Who lives where and their attributes
*/

select
  -- Keys
  dem.person_id,
  {{ dbt_utils.generate_surrogate_key(['dem.uprn_hash']) }} as household_id,
  dem.uprn_hash,

  -- Person demographics and status
  dem.is_active,
  dem.is_deceased,
  dem.sex,
  dem.age,
  dem.age_life_stage,
  dem.age_band_ons,

  -- Ethnicity and language
  dem.ethnicity_category,
  dem.main_language,
  dem.interpreter_needed,

  -- Practice registration
  dem.current_practice_code,
  dem.current_practice_name,
  dem.registration_start_date,

  -- Practice organisational context
  dem.pcn_code,
  dem.pcn_name,
  dem.local_authority,
  dem.practice_neighbourhood,

  -- Temporal context
  current_date() as snapshot_date,
  current_timestamp() as created_at

from {{ ref('dim_person_demographics') }} dem
where dem.uprn_hash is not null
