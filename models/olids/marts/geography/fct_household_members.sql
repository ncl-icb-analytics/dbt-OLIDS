{{
  config(
    materialized='table',
    tags=['household', 'geography', 'bridge', 'members'])
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
  dem.practice_code,
  dem.practice_name,
  dem.registration_start_date,

  -- Practice organisational context
  dem.pcn_code,
  dem.pcn_name,
  dem.borough_registered,
  dem.neighbourhood_registered,

  -- Temporal context
  current_date() as snapshot_date,
  current_timestamp() as created_at

from {{ ref('dim_person_demographics') }} dem
where dem.uprn_hash is not null
